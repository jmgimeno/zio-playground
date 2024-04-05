import zio.*

object FiberZip extends ZIOAppDefault {

  val program1 = for {
    fib1 <- (ZIO.debug("fiber1") *> ZIO.sleep(5.seconds) *> ZIO.debug("fib1 finished") *> ZIO.fail(1)).onInterrupt(ZIO.debug("fiber1 interrupted")).fork
    fib2 <- (ZIO.debug("fiber2") *> ZIO.sleep(10.seconds) *> ZIO.debug("fib2 finished") *> ZIO.fail(2)).onInterrupt(ZIO.debug("fiber2 interrupted")).fork
    res <- ZIO.collectAllPar(List(fib1.join, fib2.join))
    // fiball = fib1.zip(fib2)
    _ <- ZIO.sleep(2.seconds)
    // _ <- ZIO.debug("interrupt fiball") *> fiball.interrupt
    // _ <- ZIO.debug("interrupt fib1") *> fib1.interrupt
    // res <- fiball.join./*parallelErrors.*/catchAll(errors => ZIO.debug(s"errors $errors"))
    _ <- ZIO.debug(s"result $res")
  } yield ()

  val run = program1
}
