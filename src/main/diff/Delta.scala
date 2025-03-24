package diff

import cats.instances.char._
import cats.instances.double._
import cats.instances.option._
import cats.instances.string._
import cats.syntax.either._
import cats.syntax.eq._
import cats.syntax.option._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import instances._

/**
 * Represents intermediate delta information for one pair of rows.
 * @param keys the values for the composite primary key
 * @param deltas the deltas produced by the comparison
 * @param diffType difference type for the two rows: whether they are the same, different,
 * missing their left-hand side or right-hand side or incomparable
 */
final case class DeltaInfo(keys: List[String], deltas: List[Delta], diffType: DiffType)

/** The type of values we're computing deltas over: either numeric or string-based. */
sealed trait DeltaType
object DeltaType {

  /** Numeric delta type. */
  final case object Numeric extends DeltaType

  /** String-based delta type. */
  final case object String extends DeltaType
}

/** The numeric value of the delta between two values. */
sealed trait DeltaValue
object DeltaValue {

  /**
   * Represents a value for which we can't compute the delta, this is the case for nested types
   * for example.
   * @param sparkType which is not supported
   */
  final case class UnsupportedDelta(sparkType: String) extends DeltaValue

  /**
   * Represents a computed delta.
   * @param deltaType type of the delta: either numeric or string-based
   * @param value of the detla between two values
   */
  final case class TypedDelta(deltaType: DeltaType, value: Double) extends DeltaValue
  object NumericDelta {

    /**
     * Creates a numeric delta from a delta value.
     * @param value delta value computed using substraction
     * @return a numeric delta
     */
    def apply(value: Double): DeltaValue = TypedDelta(DeltaType.Numeric, value)
  }
  object StringDelta {

    /**
     * Creates a string-based delta from a delta value.
     * @param value delta value computed using the Levenshtein distance
     * @return a numeric delta
     */
    def apply(value: Double): DeltaValue = TypedDelta(DeltaType.String, value)
  }
}

/**
 * Represents a delta between two values.
 * @param fieldName name of the field from the two datasets we are comparing
 * @param left value
 * @param right value
 * @param deltaValue computed delta between left and right
 */
final case class Delta(
    fieldName: String,
    left: Option[Any],
    right: Option[Any],
    deltaValue: DeltaValue
)
object Delta {

  /**
   * Computes the deltas between two Spark rows, field by field.
   * If the schema of the two rows is not the same a left is returned.
   * Otherwise, values are compared two by two by field name to produce a list of deltas.
   * Booleans and numeric types produce a numeric delta.
   * Dates, timestamps and strings produce a string-based delta.
   * Other types (notably nested types) are not supported.
   * @param l left row
   * @param r right row
   * @return either the list of computed deltas or an error if the schema of the two rows is not
   * the same
   */
  def diff(l: Row, r: Row): Either[String, List[Delta]] =
    if (l.schema =!= r.schema) {
      s"Cannot compare rows with different schemas (left: ${l.schema}, right: ${r.schema}".asLeft
    } else {
      l.schema.fields.toList
        .map {
          case StructField(name, dt, _, _) =>
            val lValue = getValue(l, name)
            val rValue = getValue(r, name)
            dt match {
              case ByteType | DoubleType | FloatType | IntegerType | LongType | ShortType |
                  _: DecimalType =>
                val (lv, rv) = (doubleValue(lValue), doubleValue(rValue))
                if (lv === rv) None
                else Delta(name, lValue, rValue, DeltaValue.NumericDelta(numericDelta(lv, rv))).some
              case BooleanType =>
                val (lv, rv) = (booleanValue(lValue), booleanValue(rValue))
                if (lv === rv) None
                else Delta(name, lValue, rValue, DeltaValue.NumericDelta(numericDelta(lv, rv))).some
              case DateType | TimestampType | StringType =>
                val (lv, rv) = (stringValue(lValue), stringValue(rValue))
                if (lv === rv) None
                else Delta(name, lValue, rValue, DeltaValue.StringDelta(stringDelta(lv, rv))).some
              case t => Delta(name, lValue, rValue, DeltaValue.UnsupportedDelta(t.toString())).some
            }
        }
        .flatten
        .asRight
    }

  /**
   * Extracts a double value from an Any.
   * It doesn't check that the value is actually numeric, this should be checked beforehand.
   * @param a an Any
   * @return a double
   */
  def doubleValue(a: Option[Any]): Option[Double] = a.map(_.toString().toDouble)

  /**
   * Extracts a double value from an Any representing a Boolean.
   * It doesn't check that the value is actually a boolean, this should be checked beforehand.
   * @param a an Any
   * @return a double, if the boolean is false returns 0, otherwise returns 1
   */
  def booleanValue(a: Option[Any]): Option[Double] =
    a.map(_.toString().toBoolean.compare(false).toDouble)

  /**
   * Extracts a string value from an Any.
   * @param a an Any
   * @return a string
   */
  def stringValue(a: Option[Any]): Option[String] = a.map(_.toString())

  /**
   * Extracts a value from a row through its field name.
   * @param r row
   * @param fieldName name of the field to extract the value with
   * @return the value if the field name exists in the row and a value can be extracted, None if
   * the value is null.
   */
  def getValue(r: Row, fieldName: String): Option[Any] =
    (for {
      fieldIndex <- Either.catchNonFatal(r.fieldIndex(fieldName))
      value      <- Either.catchNonFatal(r.get(fieldIndex))
    } yield value).fold(_ => None, Option(_))

  /**
   * Computes a delta between two values:
   * - if both values are None, returns 0.
   * - if one value is None, returns that value.
   * - otherwise computes the detla between the two values as their difference.
   * @param l left value
   * @param r right value
   * @return a delta
   */
  def numericDelta(l: Option[Double], r: Option[Double]): Double =
    optionDelta[Double](l, r, identity, (l: Double, r: Double) => numericDelta(l, r))

  /** Computes the difference between two doubles. */
  def numericDelta(l: Double, r: Double): Double = r - l

  /**
   * Computes a delta between two values:
   * - if both values are None, returns 0.
   * - if one value is None, returns that value.
   * - otherwise computes the detla between the two values as their Levenshtein distance.
   * @param l left value
   * @param r right value
   * @return a delta
   */
  def stringDelta(l: Option[String], r: Option[String]): Double =
    optionDelta[String](l, r, _.length().toDouble, (ls, rs) => stringDelta(ls, rs))

  /** Computes the Levenshtein distance between two strings. */
  def stringDelta(s1: String, s2: String): Double = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }
    for (j <- 1 to s2.length; i <- 1 to s1.length) {
      dist(j)(i) = if (s2(j - 1) === s1(i - 1)) {
        dist(j - 1)(i - 1)
      } else {
        minimum(
          dist(j - 1)(i) + 1,
          dist(j)(i - 1) + 1,
          dist(j - 1)(i - 1) + 1
        )
      }
    }
    dist(s2.length)(s1.length).toDouble
  }

  /** Computes the minimum between three values. */
  def minimum(d1: Int, d2: Int, d3: Int): Int =
    math.min(d1, math.min(d2, d3))

  /**
   * Computes a delta between two options:
   * - if both values are None, returns 0.
   * - if one value is None, returns f of that value.
   * - otherwise, returns g of these two values.
   * @param l left value
   * @param r right value
   * @param f turns one value into a double
   * @param g turns two values into a double
   * @return a delta
   */
  def optionDelta[T](l: Option[T], r: Option[T], f: T => Double, g: (T, T) => Double): Double =
    (l, r) match {
      case (None, None)       => 0d
      case (Some(a), None)    => f(a)
      case (None, Some(a))    => f(a)
      case (Some(a), Some(b)) => g(a, b)
    }
}

