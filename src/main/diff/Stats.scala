package diff

import cats.kernel.Eq

/** Represents statistics for the whole comparison between two datasets.
  * @param numTotal
  *   total number of rows for each dataset
  * @param numSame
  *   number of rows which are the same for the two datasets
  * @param numDiff
  *   number of rows which are different in the two datasets
  * @param numMissingLHS
  *   number of rows which are missing in the left dataset
  * @param numMissingRHS
  *   number of rows which are missing in the right dataset
  * @param numIncomparable
  *   number of rows which were not comparable
  */
final case class GlobalStats(
    numTotal: Long,
    numSame: Long,
    numDiff: Long,
    numMissingLHS: Long,
    numMissingRHS: Long,
    numIncomparable: Long
)

object GlobalStats {
  implicit val globalStatsEq: Eq[GlobalStats] = Eq.fromUniversalEquals
}

/** Represents different statistics for the distribution of the detlas for a field.
  * @param deltaType
  *   type of delta which was computed either numeric or string-based (Levenshtein distance)
  * @param min
  *   minimum delta value
  * @param max
  *   maximum delta value
  * @param count
  *   number of deltas these statistics were aggregated over
  * @param mean
  *   first moment for the deltas
  * @param variance
  *   second moment for the deltas
  * @param stddev
  *   square root of the second moment for the deltas
  * @param skewness
  *   third moment for the deltas
  * @param kurtosis
  *   fourth moment for the deltas
  */
final case class DeltaStats(
    deltaType: String,
    min: Double,
    max: Double,
    count: Long,
    mean: Double,
    variance: Double,
    stddev: Double,
    skewness: Double,
    kurtosis: Double
)

/** Represents the statistics for a particular field.
  * @param fieldName
  *   name of the field these deltas were computed over
  * @param count
  *   number of different values for this field
  * @param fraction
  *   number of different values for this field over the total number of different rows
  * @param deltaStats
  *   statistics for the computed deltas
  */
final case class FieldStats(
    fieldName: String,
    count: Long,
    fraction: Double,
    deltaStats: Option[DeltaStats]
)

/** Represents the computed statistics.
  * @param global
  *   statistics on the whole comparison
  * @param fields
  *   statistics by field
  */
final case class Stats(global: GlobalStats, fields: List[FieldStats])
