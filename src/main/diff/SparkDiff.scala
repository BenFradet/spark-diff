package diff

import cats.effect.kernel.Sync
import cats.syntax.applicative._
import cats.syntax.either._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.twitter.algebird._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import instances._
import types.MapVal

object SparkDiff {
  def diff[F[_]: Sync](
      lhs: DataFrame,
      rhs: DataFrame,
      compositePrimaryKeys: Set[String]
  ): F[Either[String, Stats]] =
    for {
      deltas <- computeDeltas(lhs, rhs, compositePrimaryKeys)
      stats  <- deltas match {
        case Right(ds) => computeStats(ds).map(o => o.toRight("no elements"))
        case Left(o)   => Left(o).pure[F]
      }
    } yield stats

  /** Computes the deltas between two DataFrames. It extracts the keys from each dataset through the supplied composite
    * primary keys. All the data is then unioned over and grouped by the previously extracted key. Delta information is
    * then computed for each pair of Rows.
    * @param lhs
    *   left-hand DataFrame
    * @param rhs
    *   right-hand DataFrame
    * @param compositePrimaryKeys
    *   set of columns which forms a primary key
    * @return
    *   an rdd containing all delta infos, there is one delta info per compared pair of Rows.
    */
  def computeDeltas[F[_]: Sync](
      lhs: DataFrame,
      rhs: DataFrame,
      compositePrimaryKeys: Set[String]
  ): F[Either[String, RDD[DeltaInfo]]] =
    for {
      lSchema <- lhs.schema.pure[F]
      rSchema <- rhs.schema.pure[F]
      kFn = keyFn(compositePrimaryKeys)
      lRDD   <- Sync[F].delay(lhs.rdd.map(r => (kFn(r), ("l", r))))
      rRDD   <- Sync[F].delay(rhs.rdd.map(r => (kFn(r), ("r", r))))
      deltas <- Sync[F].delay {
        (lRDD ++ rRDD)
          .groupByKey()
          .map { case (keys, values) => getDeltaInfo(keys, values.toList) }
      }
    } yield (if (lSchema === rSchema) {
               deltas.asRight
             } else {
               s"LHS (${lSchema.treeString}) and RHS (${rSchema.treeString}) don't have the same schema".asLeft
             })

  /** Computes statistics based on previously computed delta information. Statistics are first initialized on a per row
    * basis and then summed over
    * @param rdd
    *   of delta information
    * @return
    *   a stats object or None if the rdd is empty.
    */
  def computeStats[F[_]: Sync](rdd: RDD[DeltaInfo]): F[Option[Stats]] = {
    implicit val deltaTypeSemigroup = new Semigroup[DeltaType] {
      override def plus(l: DeltaType, r: DeltaType): DeltaType = l
    }
    Sync[F].delay {
      rdd
        .map { case DeltaInfo(_, ds, dt) => initStats(ds, dt) }
        .algebird
        .sumOption
        .map { case (dtVec, fieldMap) => finalizeStats(dtVec, fieldMap) }
    }
  }

  /** Computes deltas for the provided pair of rows. If there are more than two values for this key, the key is not a
    * composite primary key so the rows are not comparable. If there are less than two values for this key, we're
    * missing left or right. If there are two values for this key, we compare them.
    * @param keys
    *   values corresponding to the composite primary keys for this table
    * @param values
    *   list we should contain two elements (one for lhs, one for rhs) tagged with "l" or "r"
    * @return
    *   delta information
    */
  def getDeltaInfo(keys: List[String], values: List[(String, Row)]): DeltaInfo = {
    val map = values.toMap
    map.size match {
      case i if i > 2 =>
        val msg =
          s"More than 2 values found for key: ${keys.mkString(", ")}, it should be a primary key"
        DeltaInfo(keys, Nil, DiffType.Incomparable(msg))
      case 2 =>
        (map.get("l"), map.get("r")) match {
          case (Some(l), Some(r)) =>
            Delta.diff(l, r) match {
              case Left(a)   => DeltaInfo(keys, Nil, DiffType.Incomparable(a))
              case Right(ds) =>
                val diffType = if (ds.isEmpty) DiffType.Same else DiffType.Different
                DeltaInfo(keys, ds, diffType)
            }
          case _ =>
            val msg = s"Could not identify lhs and rhs for key: ${keys.mkString(", ")}"
            DeltaInfo(keys, Nil, DiffType.Incomparable(msg))
        }
      case _ =>
        val diffType = if (map.contains("l")) DiffType.MissingRHS else DiffType.MissingLHS
        DeltaInfo(keys, Nil, diffType)
    }
  }

  /** Initializes the statistics per list of deltas outputted by a row comparison
    * @param ds
    *   list of deltas produced by the comparison of two rows.
    * @param dt
    *   the type of difference between these two rows.
    * @return
    *   counts per diff type and initialized algebird stats (min, max and moments) per field
    */
  def initStats(
      ds: List[Delta],
      dt: DiffType
  ): ((Long, Long, Long, Long, Long, Long), Map[String, MapVal]) = {
    val m = ds.foldLeft(Map.empty[String, MapVal]) { case (acc, d) =>
      val optDelta = d.deltaValue match {
        case _: DeltaValue.UnsupportedDelta => None
        case DeltaValue.TypedDelta(t, v)    =>
          Some((t, Min(v), Max(v), Moments.aggregator.prepare(v)))
      }
      acc.+((d.fieldName, (1L, optDelta)))
    }
    val dtVec = dt match {
      case DiffType.Same            => (1L, 1L, 0L, 0L, 0L, 0L)
      case DiffType.Different       => (1L, 0L, 1L, 0L, 0L, 0L)
      case DiffType.MissingLHS      => (1L, 0L, 0L, 1L, 0L, 0L)
      case DiffType.MissingRHS      => (1L, 0L, 0L, 0L, 1L, 0L)
      case _: DiffType.Incomparable => (1L, 0L, 0L, 0L, 0L, 1L)
    }
    (dtVec, m)
  }

  /** Finalize all statistics into a Stats object.
    * @todo
    *   deltaType is a string because there are no encoders for ADTs in Spark
    * @param vec
    *   the summed over counts per diff type
    * @param fieldMap
    *   aggregated algebird stats (min, max and moments) per field
    * @return
    *   a final stats object
    */
  def finalizeStats(
      vec: (Long, Long, Long, Long, Long, Long),
      fieldMap: Map[String, MapVal]
  ): Stats = {
    val global = (GlobalStats.apply _).tupled(vec)
    val field  = fieldMap.map { case (field, (count, optD)) =>
      val deltaStats = optD.map { case (dt, min, max, m) =>
        DeltaStats(
          deltaType = dt.toString(),
          min = min.get,
          max = max.get,
          count = m.count,
          mean = m.mean,
          variance = m.variance,
          stddev = m.stddev,
          skewness = m.skewness,
          kurtosis = m.kurtosis
        )
      }
      FieldStats(field, count, count.toDouble / global.numDiff, deltaStats)
    }
    Stats(global, field.toList)
  }

  /** Extracts the values corresponding to composite primary keys.
    * @param keys
    *   composite primary key forming a primary key when taken together
    * @return
    *   a function to extract the values of the composite primary keys from a Row.
    */
  def keyFn(keys: Set[String]): Row => List[String] =
    (r: Row) =>
      keys.toList.map { k =>
        val e = for {
          index <- Either.catchNonFatal(r.fieldIndex(k))
          value <- Either.catchNonFatal(r.get(index))
        } yield value
        e.fold(_ => None, Option(_).map(_.toString))
      }.flatten
}
