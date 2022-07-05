package streams

import zio.*

object Channeling extends ZIOAppDefault :

  // ZIO Streams — Part 3 — Channels
  // https://youtu.be/trF44bGHwXg

  // A stream incrementally produce a bunch of values from something (e.g. an iterator, a file, ...)
  //
  // final case class ZStream[-R, +E, +A](
  //     process: ZIO[R with Scope, E, ZIO[R, Option[E], A]])

  // A sink is the "dual" of a stream that consumes a bunch of values to produce a summary value
  //
  // final case class ZSink[-R, +E, -I, +O](
  //     run: ZIO[R with Scope, E, Chunk[I] => ZIO[R, E, Option[O]]])

  type ??? = Nothing

  object Old:

    // A channel unifies streams and sinks
    // NOTES:
    // - Transformation of the stream type
    //    - ZIO[Env with Scope, OutErr, ZIO[Env, Option[OutErr], Chunk[OutElem]]]
    //    - ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Option[Chunk[OutElem]]]]
    //    - ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[Chunk[OutElem], Unit]]]
    //    - Generalize Unit (or Any) to a type parameter for signaling the end of the stream
    //    - ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[Chunk[OutElem], OutDone]]]
    // - Transformation of the sink type
    //    - Simply rename

    final case class ZStream[-Env, +OutErr, +OutElem, +OutDone]
    (pull: ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[Chunk[OutElem], OutDone]]])

    final case class ZSink[-Env, +OutErr, -InElem, +OutDone]
    (push: ZIO[Env with Scope, OutErr, Chunk[InElem] => ZIO[Env, OutErr, Option[OutDone]]])

  end Old

  // We compare:
  //  - Both have Env
  //  - We have out versions of err, elem and done (OutErr, OutElem and OutDone)
  //  - We only have the elem version of in (InElem)
  // So, maybe:
  //  - Are there InError and InDone?
  // And we have pull and we have push
  //  - What would it mean combining those two and have something that has both a pull & push?

  // So a channel is something you can pull values from and push values to.
  //   - E.g. java.nio.AsynchronousFileChannel

  // How do we represent this?
  //
  //   - final case class ZChannel[-Env, -InErr, -InElem, -InDone, +OutErr, +OutElem, +OutDone]
  //       (pull: ???, push: ???)
  //     But here the pull/push do not interact with each other => Every op needs an internal queue
  //
  //   - We can think of a channel as something that we can pull from as long as we give it the
  //   upstream that we want the channel to process
  //     NOTE: We'll ignore the chunking at the channel level
  //     - The stream part of the channel can be represented as:
  //       (run: ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]])
  //     - But, as we've said, we need an upstream to do this pull, so channel is saying: if you give me some
  //       upstream I can go ahead and you'll be able to pull from me, and I'll perform the logic of when I
  //       want to pull from upstream or what I'm gonna do and will eventually give the value that you're pulling from
  //       (run: ZIO[Env with Scope, OutErr, IO[InErr, Either[InElem, InDone]] => ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]]])
  //     - An the same way we have the channel being managed, the upstream can be managed as well
  //       (run: ZIO[Env with Scope, OutErr, ZIO[Scope, InErr, IO[InErr, Either[InElem, InDone]]] => ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]]])
  //     - NOTE: upstream  does not get the Env (it doesn't compile if it takes it) but, as it's accessible in the outer managed resource
  //       we can provide it if we need it.
  //     - Instead of a managed function, we only have a function that says: you give me the upstream you want to pull from and I will give you something
  //       that you can open and then pull from
  //       (run: ZIO[Scope, InErr, IO[InErr, Either[InElem, InDone]]] => ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]])

  final case class ZChannel[-Env, -InErr, -InElem, -InDone, +OutErr, +OutElem, +OutDone]
    (run: ZIO[Scope, InErr, IO[InErr, Either[InElem, InDone]]] => ZIO[Env with Scope, OutErr, ZIO[Env, OutErr, Either[OutElem, OutDone]]]):

    // Whatever the upstrem we give it, this produces a thing we can pull from, and every time we pull from it, the element
    // will be transformed by the function
    def mapElem[OutElem2](f: OutElem => OutElem2): ZChannel[Env, InErr, InElem, InDone, OutErr, OutElem2, OutDone] =
      ZChannel { upstream =>
        run(upstream).map(_.map(_.left.map(f)))
      }

  end ZChannel


  object ZChannel:

    def fromIteratorChunk[A](iterator: => Iterator[A], chunkSize: Int = 1): ZChannel[Any, Any, Any, Any, Nothing, Chunk[A], Any] =
      ZChannel { _ =>
        ZIO.succeed(iterator.grouped(chunkSize)).map { iterator =>
          ZIO.succeed(iterator.hasNext).map { b =>
            if b then Left(Chunk.fromIterable(iterator.next()))
            else Right(())
          }
        }
      }

  end ZChannel

  // A stream just produces values
  // - so it does not care what its inputs are (so the In-tyoes are Any)
  // - the OutErr will be E
  // - the OutElem will be Chunk[A] (we recover chunkiness)
  // - and we can use anything for signaling the end of the stream (OutDone)

  final case class ZStream[-R, +E, +A](channel: ZChannel[R, Any, Any, Any, E, Chunk[A], Any]):

    def map[B](f: A => B): ZStream[R, E, B] =
      ZStream {
        channel.mapElem(_.map(f))
      }

  end ZStream

  object ZStream:

    //    def fromIterator[A](iterator: Iterator[A]): ZStream[Any, Nothing, A] =
    //      ZStream {
    //        ZChannel { upstream =>
    //          ???
    //        }
    //      }
    // But we can implement this at the channel level

    def fromIterator[A](iterator: Iterator[A], chunkSize: Int = 1): ZStream[Any, Nothing, A] =
      ZStream {
        ZChannel.fromIteratorChunk(iterator, chunkSize)
      }

  end ZStream

  // Some comments on variance:
  // - In-types (contravariant) => requirements
  // - Out-types (covariant) => expectations
  // Requirements have a minimum expectation, the more you know, the more you expect of a type, and the less you can accept
  // Expectations get maximally vague, the more you return, the less you know but the more you can accept

  def run = ???

end Channeling