import sbt.{Compile, Configuration => _, Test, TestFrameworks, Tests}
import sbt.Keys._
import sbt.io.syntax._

object Settings {
  val settings = Seq(
    name                 := "spark-diff",
    version              := "0.1.0-SNAPSHOT",
    scalaVersion         := "2.13.16",

    // Custom folders path (remove the `/scala` default subdirectory)
    Compile / scalaSource := file((baseDirectory.value / "src" / "main").toString),
    Test / scalaSource    := file((baseDirectory.value / "src" / "test").toString),

    // Test options
    Test / parallelExecution  := false,
    Test / testForkedParallel := false,
    Test / fork               := true,
  )
}
