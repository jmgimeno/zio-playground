import zio.{Console, ZIOAppDefault}

object HelloWorld extends ZIOAppDefault :

  val run = Console.printLine("Hello, from ZIO!")


end HelloWorld
