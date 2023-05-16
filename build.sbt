val scala3Version = "3.3.0-RC4"

enablePlugins(ScalaNativePlugin)

// set to Debug for compilation details (Info is default)
logLevel := Level.Info

// import to add Scala Native options
import scala.scalanative.build._

lazy val root = project
  .in(file("."))
  .settings(
    name := "Scala 3 Async Prototype",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,
    javaOptions += "--enable-preview --version 19",

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,

    // defaults set with common options shown
    nativeConfig ~= { c =>
      c.withLTO(LTO.none) // thin
        .withMode(Mode.releaseFast) // releaseFast
        .withGC(GC.immix) // commix
        .withMultithreadingSupport(true) 
    }
  )
