# spark-diff [![License](https://img.shields.io/github/license/benfradet/spark-diff?style=flat-square)](/LICENSE) [![Continuous Integration status](https://img.shields.io/github/actions/workflow/status/benfradet/spark-diff/ci.yml?style=flat-square)](https://github.com/benfradet/spark-diff/actions/workflows/ci.yml)

Comparing Spark Dataframes.

## Usage

```scala
import cats.effect.IO
import cats.instances.all._

val df1: DataFrame = ???
val df2: DataFrame = ???
for {
  result <- SparkDiff.diff[IO](df1, df2, Set.empty) // Either[String, Stats] 
  _ = result match {
    case Left(err) => IO.println(err)
    case Right(stats) => {
      IO.println(s"total number of rows: ${stats.global.numTotal}") *>
        IO.println(s"total number of same rows: ${stats.global.numSame}") *>
        IO.println(s"total number of different rows: ${stats.global.numDiff}") *>
        IO.println(s"number of rows missing on the left hand side: ${stats.global.numMissingLHS}") *>
        IO.println(s"number of rows missing on the right hand side: ${stats.global.numMissingRHS}") *>
        IO.println(s"number of incomparable rows: ${stats.global.numIncomparable}") *>
        IO.println("") *>
        stats.fields.map { fs =>
          IO.println(s"field name: ${fs.fieldName}") *>
          IO.println(s"number of different values for this field: ${fs.count}") *>
          IO.println(s"number of different values for this field over the total number of rows: ${fs.fraction}") *>
          IO.println(s"statistical moments: ${fs.deltaStats}") *>
          IO.println("")
        }.sequence
    }
  }
} yield ()
```
