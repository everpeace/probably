import Dependencies._

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "com.github.everpeace",
        scalaVersion := "2.12.3",
        crossScalaVersions := Seq("2.11.11", "2.12.3"),
        version := "0.1.0-SNAPSHOT",
        scalafmtOnCompile := true
      )
    )
  )
  .aggregate(
    core
  )

lazy val core = (project in file("core"))
  .settings(
    name := "probably-core",
    scalapropsVersion := "0.5.1",
    libraryDependencies ++= Seq(scalaTest % Test)
  )
  .settings(
    scalapropsSettings
  )
