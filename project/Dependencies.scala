import sbt._

object Dependencies {
  val all = Seq(
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.typelevel" %% "cats-core" % Versions.Cats,
    "org.typelevel" %% "cats-effect" % Versions.CatsEffect,
    "com.twitter" %% "algebird-core" % Versions.Algebird,
    "com.disneystreaming" %% "weaver-cats" % Versions.Weaver % Test,
    "com.disneystreaming" %% "weaver-scalacheck" % Versions.Weaver % Test,
  )
}

object Versions {
  val Algebird = "0.13.10"
  val Cats = "2.13.0"
  val CatsEffect = "3.6.0"
  val Spark = "3.5.5"
  val Weaver = "0.8.3"
}
