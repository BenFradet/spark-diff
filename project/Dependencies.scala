import sbt._

object Dependencies {
  val all = Seq(
    "org.apache.spark" %% "spark-sql"         % Versions.Spark  % Provided,
    "org.typelevel"    %% "cats-core"         % Versions.Cats,
    "org.typelevel"    %% "cats-effect"       % Versions.CatsEffect,
    "com.twitter"      %% "algebird-core"     % Versions.Algebird,
    "org.typelevel"    %% "weaver-cats"       % Versions.Weaver % Test,
    "org.typelevel"    %% "weaver-scalacheck" % Versions.Weaver % Test
  )
}

object Versions {
  val Algebird   = "0.13.10"
  val Cats       = "2.13.0"
  val CatsEffect = "3.6.3"
  val Spark      = "4.1.1"
  val Weaver     = "0.11.3"
}
