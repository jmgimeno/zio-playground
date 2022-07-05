package streams

import zio.*

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}

object Streaming extends ZIOAppDefault :

  // ZIO Stream — Part 1 — Introduction (https://youtu.be/y21EnJ28mpM)
  // ZIO Stream - Part 2 - Sinks! (https://youtu.be/T5vBs6_W_Xg)

  /*
    - Streams are incremental
    - Streams unfold over time
    - ZIO yields ZERO or ONE result
    - ZStream yields ZERO or ONE or TWO or THREE or ... results
  */
  val myInt =
    ZIO.succeed(42)

  val myFakeStream =
    Console
      .printLine(42)
      .delay(100.milliseconds)
      .repeat(Schedule.recurs(10))
      .as(42)
      .debug("FINAL OUTPUT")

  val myFakeStream2 =
    ZIO.succeed(Chunk(42, 56, 78))

  // Iterator
  trait Iterator[+A]:
    def hasNext: Boolean // Option[E]

    def next(): A // ZIO[A]

    def foreach(f: A => Unit): Unit =
      while hasNext do f(next())
  end Iterator

  object Iterator:
    def fromList[A](list: List[A]): Iterator[A] = // ZIO[E with Scope, ...]
      new :
        private var current = list

        def hasNext: Boolean = current.nonEmpty

        def next(): A =
          val head = current.head
          current = current.tail
          head

    //val x = fromList(List(1, 2, 3)) // side effect of allocating mutable state
    //val y = fromList(List(1, 2, 3))
    //x.foreach(println)
    //x.foreach(println)
  end Iterator

  //  val zio1: ZIO[Any, String, Option[Int]] = ???
  //  val zio2: ZIO[Any, Option[String], Int] = zio1.some

  // Effectual iterator
  // In reality the stream is chunked so A is Chunk[A]

  // Protocol:
  // - Succeed with A -> emit a value of type E
  // - Fail with None -> end of stream
  // - Fail with Some(E) -> fail with an E
  final case class ZStream[-R, +E, +A](process: ZIO[R with Scope, E, ZIO[R, Option[E], A]]):

    import ZStream.Pull

    def tap(f: A => UIO[Any]): ZStream[R, E, A] =
      ZStream {
        process.map(_.tap(f))
      }

    def map[B](f: A => B): ZStream[R, E, B] =
      ZStream {
        process.map(_.map(f))
      }

    def take(n: Int): ZStream[R, E, A] =
      ZStream {
        Ref.make(0).zip(process).map { (ref, pull) =>
          ref.getAndUpdate(_ + 1).flatMap { i =>
            if i >= n then Pull.end
            else pull
          }
        }
      }

    def drop(n: Int): ZStream[R, E, A] =
      ZStream {
        Ref.make(0).zip(process).map { (ref, pull) =>
          lazy val skipN: ZIO[R, Option[E], A] =
            ref.get.flatMap { i =>
              if i >= n then pull
              else ref.update(_ + 1) *> pull *> skipN
            }
          skipN
        }
      }

    def dropAlt(n: Int): ZStream[R, E, A] =
      ZStream {
        Ref.make(0).zip(process).map { (ref, pull) =>
          lazy val skipN: ZIO[R, Option[E], A] =
            pull.flatMap { a =>
              ref.get.flatMap { i =>
                if i >= n then Pull.emit(a)
                else ref.update(_ + 1) *> skipN
              }
            }
          skipN
        }
      }

    def run[R1 <: R, E1 >: E, O](sink: ZSink[R1, E1, A, O]): ZIO[R1, E1, O] =
      ZIO.scoped {
        process.zip(sink.run).flatMap { (pull, push) =>
          lazy val loop: ZIO[R1, E1, O] =
            pull.foldZIO(
              {
                case Some(e) => ZIO.fail(e)
                case None => push(Chunk.empty).flatMap {
                  case Some(o) => ZIO.succeed(o)
                  case None => ZIO.dieMessage("Sink violated contract by returning None after being pushed empty Chunk")
                }
              },
              a => push(Chunk.single(a)).flatMap {
                case Some(o) => ZIO.succeed(o)
                case None => loop
              }
            )
          loop
        }
      }

    def runCollect: ZIO[R, E, Chunk[A]] =
      ZIO.scoped {
        process.flatMap { pull =>
          val builder = ChunkBuilder.make[A]()
          lazy val loop: ZIO[R, E, Chunk[A]] =
            pull.foldZIO(
              {
                case Some(e) => ZIO.fail(e) // stream failed with error
                case None => ZIO.succeed(builder.result()) // stream ended
              },
              a => ZIO.succeed(builder += a) *> loop // stream emitted element
            )
          loop
        }
      }

    def runToFile(name: String)(using ev: A <:< String): ZIO[R, E, Unit] =
      ZIO.scoped {
        process.flatMap { pull =>
          ZIO.succeed(BufferedWriter(FileWriter(name))).flatMap { writer =>
            lazy val loop: ZIO[R, E, Unit] =
              pull.foldZIO(
                {
                  case Some(e) => ZIO.fail(e)
                  case None => ZIO.succeed(())
                },
                a => ZIO.succeed {
                  writer.write(a)
                  writer.newLine()
                } *> loop
              )
            loop.ensuring(ZIO.succeed(writer.close()))
          }
        }
      }

  end ZStream

  object ZStream:

    object Pull:
      def emit[A](a: A) = ZIO.succeed(a)

      val end = ZIO.fail(None)
    end Pull

    def fromIterator[A](iterator: => Iterator[A]): ZStream[Any, Nothing, A] =
      ZStream {
        ZIO.succeed(iterator).map { iterator =>
          ZIO.succeed(iterator.hasNext).flatMap { b =>
            if b then Pull.emit(iterator.next())
            else Pull.end // END OF STREAM
          }
        }
      }

    def lines(file: String): ZStream[Any, Throwable, String] =
      ZStream {
        ZIO.acquireRelease(
          ZIO.succeed(BufferedReader(FileReader(file)))
        )(reader => ZIO.succeed(reader.close())).map { reader =>
          ZIO.attempt(reader.readLine()).asSomeError.flatMap { line =>
            if line ne null then Pull.emit(line) else Pull.end
          }
        }
      }

  end ZStream

  // ZIO[R, E, A]
  // ZStream[R, E, A]
  // In --> Processing --> Out
  // Fan Out // broadcast operations

  // In -----> Processing1 -----> Out
  //    \----> Processing2 -----/

  // In == Stream // create values from "nothing" (something hidden in the Stream or ZIO)
  // () => A
  // ZIO[R, E, A]
  // ZStream[R, E, A]

  // A => Z  // consumes ONE value
  // A => ZIO[R, E, B] == continuation
  // zio.flatMap(f)

  // A => Z  // consumes MANY values
  // ZSink

  // final case class ZStream[-R, +E, +A](
  //   process: ZIO[R with Scope, E, ZIO[R, Option[E], A]])

  // Protocol:
  // - Succeed with Some(O) means "I'm done with a summary value of O"
  // - Succeed with None means "feed me more input"
  // - Fail with E means "I'm done with an error E"
  // A complication will not address are "leftovers" -> add a parameter for leftovers

  final case class ZSink[-R, +E, -I, +O](run: ZIO[R with Scope, E, Chunk[I] => ZIO[R, E, Option[O]]]):
    self =>

    def zipWithPar[R1 <: R, E1 >: E, I1 <: I, O2, O3](that: ZSink[R1, E1, I1, O2])(f: (O, O2) => O3): ZSink[R1, E1, I1, O3] =

      enum State[+O, +O2]:
        case Running
        case LeftDone(o: O)
        case RightDone(o2: O2)

      import State.*

      ZSink {
        self.run
          .zipPar(that.run)
          .zipPar(Ref.make[State[O, O2]](Running))
          .map { case (pushLeft, pushRight, ref) =>
            in =>
              ref.get.flatMap {
                case Running =>
                  pushLeft(in).zipPar(pushRight(in)).flatMap {
                    case (Some(o), Some(o2)) => ZIO.succeed(Some(f(o, o2)))
                    case (Some(o), None) => ref.set(LeftDone(o)).as(None)
                    case (None, Some(o2)) => ref.set(RightDone(o2)).as(None)
                    case (None, None) => ZIO.succeed(None)
                  }
                case LeftDone(o) =>
                  pushRight(in).map {
                    case Some(o2) => Some(f(o, o2))
                    case None => None
                  }
                case RightDone(o2) =>
                  pushLeft(in).map {
                    case Some(o) => Some(f(o, o2))
                    case None => None
                  }
              }
          }
      }

  object ZSink:

    def runCollect[A]: ZSink[Any, Nothing, A, Chunk[A]] =
      ZSink {
        Ref.make(Chunk.empty[A]).map { ref =>
          in =>
            if in.isEmpty
            then ref.get.asSome
            else ref.update(_ ++ in).as(None)
        }
      }

    private def writer(file: String): ZIO[Scope, Nothing, BufferedWriter] =
      ZIO.acquireRelease(
        ZIO.succeed(BufferedWriter(FileWriter(file)))
      )(writer => ZIO.succeed(writer.close()))

    def toFile(file: String): ZSink[Any, Throwable, String, Unit] =
      ZSink {
        writer(file).map { writer =>
          in =>
            if in.isEmpty
            then ZIO.succeed(Some(()))
            else ZIO.foreachDiscard(in) {
              s =>
                ZIO.succeed {
                  writer.write(s)
                  writer.newLine()
                }
            }.as(None)
        }
      }

  end ZSink

  //  trait StreamSupervisor[R, E, A, B]:
  //    def run(stream: ZStream[R, E, A], sink: ZSink[R, E, A, B]): ZIO[R, E, B]


  val myRealStream = ZStream.fromIterator(Iterator.fromList(List(1, 2, 3, 4, 5, 6, 7)))

  //  val run =
  //    fileStream.runCollect
  //    for {
  //      chunk <- myRealStream
  //        .dropAlt(2)
  //        .tap(a => ZIO.debug(s"WOW $a"))
  //        .map(_ * 2)
  //        .tap(a => ZIO.debug(s"BIGGER WOW $a"))
  //        .take(3)
  //        .tap(_ => ZIO.sleep(1.second))
  //        .runCollect
  //      _ <- ZIO.debug(chunk)
  //      _ <- second
  //    } yield ()

  lazy val second = ZIO.succeed {
    List(1, 2, 3, 4, 5, 6, 7)
      .map(a => {
        println(s"LIST WOW $a")
        a
      })
      .map(_ * 2)
      .map(a => {
        println(s"LIST BIGGER WOW $a")
        a
      })
      .take(3)
  }

  lazy val fileStreamProcess =
    ZStream
      .lines("cool.txt") // in
      .tap(a => ZIO.debug(s"WOW FROM FILE $a").delay(1.second)) // middle
      .runCollect // end

  // in is describable as a value
  // but middle and end are not (they are operators)
  // => this puts limits on composability

  val run0 = myRealStream.map(_.toString).runToFile("cooler.txt")

  val simpleStream =
    ZStream.fromIterator(Iterator.fromList(List(1, 2, 3, 4, 5, 6, 7)))

  val simpleSink =
    ZSink.runCollect[String]

  val simpleSink2 =
    ZSink.toFile("cool2.txt")

  val notSoSimpleSink =
    simpleSink.zipWithPar(simpleSink2) { case (l, _) => l }

  val simpleStreamProgram =
    simpleStream.map(_ * 3).map(_.toString).run(simpleSink2).debug

  val notSoSimpleStreamProgram =
    simpleStream.map(_ * 3).map(_.toString).run(notSoSimpleSink).debug

  val exampleRunCollect =
    simpleStream.map(_ * 3).runCollect.debug

  val run = exampleRunCollect // notSoSimpleStreamProgram

end Streaming


