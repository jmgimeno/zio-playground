package streams

import zio.*

object Streaming extends ZIOAppDefault :

  // ZIO Stream — Part 1 — Introduction (https://youtu.be/y21EnJ28mpM)

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
  //  val zio3: ZIO[Any, Nothing, Either[String, Int]] = ???

  // Effectual iterator
  // In reality the stream is chunked so A is Chunk[A]
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

    import java.io.{BufferedReader, FileReader}

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

  val myRealStream = ZStream.fromIterator(Iterator.fromList(List(1, 2, 3, 4, 5, 6, 7)))

  val run =
    fileStream.runCollect
  //    for {
  //      chunk <- myRealStream
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

  lazy val fileStream =
    ZStream
      .lines("cool.txt")
      .tap(a => ZIO.debug(s"WOW FROM FILE $a").delay(1.second))

end Streaming


