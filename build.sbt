ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"

fork := true

lazy val root = (project in file("."))
  .settings(
    name := "zio-playground",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      "dev.zio" %% "zio-test" % "2.0.21"
    ),
    testFrameworks := Seq(TestFramework("zio.test.sbt.ZTestFramework"))
  )
