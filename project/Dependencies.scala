import sbt._

object Dependencies {
  val all = Seq(
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.typelevel" %% "cats-core" % Versions.Cats,
    "org.typelevel" %% "cats-effect" % Versions.CatsEffect,
    "com.twitter" %% "algebird-core" % Versions.Algebird,
    "org.scalacheck" %% "scalacheck" % Versions.Scalacheck % Test,
  )
}

object Versions {
  val Algebird = "0.13.10"
  val Cats = "2.13.0"
  val CatsEffect = "3.6.0"
  val Scalacheck = "1.14.1"
  val Spark = "3.5.5"
}
