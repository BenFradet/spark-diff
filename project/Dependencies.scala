import sbt._

object Dependencies {
  val all = Seq(
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.typelevel" %% "cats-core" % Versions.Cats,
  )
}

object Versions {
  val Cats = "2.13.0"
  val Spark = "3.5.5"
}
