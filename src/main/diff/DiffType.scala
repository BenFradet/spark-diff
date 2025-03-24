package diff

/** Represents the type of difference between two Spark Rows. */
sealed trait DiffType
object DiffType {

  /** When two Rows are the same. */
  final case object Same extends DiffType

  /** When two Rows are different. */
  final case object Different extends DiffType

  /** When two rows are not comparable, with the given reason. */
  final case class Incomparable(reason: String) extends DiffType

  /** When the left Row is null. */
  final case object MissingLHS extends DiffType

  /** When the right Row is null. */
  final case object MissingRHS extends DiffType
}
