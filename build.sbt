ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "zio-playground",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.0",
      "dev.zio" %% "zio-test" % "2.0.0"
    ),
    testFrameworks := Seq(TestFramework("zio.test.sbt.ZTestFramework"))
  )
