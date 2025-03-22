import sbt._

object Dependencies {
  val all = Seq(
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
  )
}

object Versions {
  val Spark = "3.5.5"
}
