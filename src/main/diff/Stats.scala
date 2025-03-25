package diff

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

/** Represents the outputted global statistics as they are on Hive.
  * @param tableName
  *   name of the table compared
  * @param numTotal
  *   total number of rows between the two tables
  * @param numSame
  *   number of rows which are the same between the two tables
  * @param numDiff
  *   number of rows which are different between the two tables
  * @param numMissingLHS
  *   number of rows for which a primary key matches no records on the left side
  * @param numMissingRHS
  *   number of rows for which a primary key matches no records on the right side
  * @param numIncomparable
  *   number of rows which are not comparable due to an issue
  */
final case class OutputGlobalStats(
    tableName: String,
    numTotal: Long,
    numSame: Long,
    numDiff: Long,
    numMissingLHS: Long,
    numMissingRHS: Long,
    numIncomparable: Long
)
object OutputGlobalStats {

  /** Builds the output global statistics from global stats.
    * @param tableName
    *   name of the table compared
    * @param global
    *   statistics
    * @return
    *   global statistics ready to be written into Hive
    */
  def apply(tableName: String, global: GlobalStats): OutputGlobalStats =
    OutputGlobalStats(
      tableName,
      global.numTotal,
      global.numSame,
      global.numDiff,
      global.numMissingLHS,
      global.numMissingRHS,
      global.numIncomparable
    )
}

/** Represents the outputted field statistics as they are on Hive.
  * @param tableName
  *   name of the table
  * @param fieldName
  *   name of the field
  * @param diffCount
  *   number of field values which are different between the two tables
  * @param diffFraction
  *   number of field values which are different between the two tables over the total number of rows which are
  *   different
  * @param deltaType
  *   type of delta which was computed to compare values
  * @param deltaMin
  *   minimum value of the deltas
  * @param deltaMax
  *   maximum value of the deltas
  * @param deltaMean
  *   first moment
  * @param deltaVariance
  *   second moment
  * @param deltaStddev
  *   square root of the second moment
  * @param deltaSkewness
  *   third moment
  * @param deltaKurtosis
  *   fourth moment
  */
final case class OutputFieldStats(
    tableName: String,
    fieldName: String,
    diffCount: Long,
    diffFraction: Double,
    deltaType: Option[String],
    deltaMin: Option[Double],
    deltaMax: Option[Double],
    deltaMean: Option[Double],
    deltaVariance: Option[Double],
    deltaStddev: Option[Double],
    deltaSkewness: Option[Double],
    deltaKurtosis: Option[Double]
)
object OutputFieldStats {

  /** Builds the output field statistics from field stats.
    * @param tableName
    *   name of the table
    * @param field
    *   statistics
    * @return
    *   field statistics ready to be written into Hive
    */
  def apply(tableName: String, field: FieldStats): OutputFieldStats =
    OutputFieldStats(
      tableName,
      field.fieldName,
      field.count,
      field.fraction,
      field.deltaStats.map(_.deltaType),
      field.deltaStats.map(_.min),
      field.deltaStats.map(_.max),
      field.deltaStats.map(_.mean),
      field.deltaStats.map(_.variance),
      field.deltaStats.map(_.stddev),
      field.deltaStats.map(_.skewness),
      field.deltaStats.map(_.kurtosis)
    )
}
