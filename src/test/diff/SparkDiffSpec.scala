package diff

import cats.Show
import cats.kernel.Eq
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}
import weaver._
import weaver.scalacheck._

import gen._
import SparkDiff._
import types.MapVal

object ProgramSpec extends SimpleIOSuite with Checkers {
  implicit val eqRow: Eq[Row]                   = Eq.fromUniversalEquals
  implicit val showRow: Show[Row]               = Show.fromToString
  implicit val showGenRow: Show[Gen[Row]]       = Show.fromToString
  implicit val showStructType: Show[StructType] = Show.fromToString
  implicit val showMapVal: Show[MapVal]         = Show.fromToString

  test("getDeltaInfo - 2 different rows with same schemas identified as l/r") {
    val stGen: Gen[(StructType, Gen[Row])] = structTypeGen(simpleStructFieldGen)
    forall(Gen.zip(Gen.listOf(nonEmptyAlphaStrGen), stGen)) { case (keys: List[String], st: (StructType, Gen[Row])) =>
      val (r1, r2) = (st._2.sample, st._2.sample)
      val values   = List(r1.map(("l", _)), r2.map(("r", _))).flatten
      val res      = getDeltaInfo(keys, values)
      res match {
        case DeltaInfo(keys, l, DiffType.Different) => expect(l.nonEmpty)
        case DeltaInfo(keys, Nil, DiffType.Same)    => expect.eql(r1.toSeq, r2.toSeq)
        case _                                      =>
          failure(s"DeltaInfo should be DeltaInfo($keys, list, DiffType.Different), got $res")
      }
    }
  }

  test("getDeltaInfo - 2 rows with different schemas identified as l/r") {
    implicit val stringArb: Arbitrary[String] = Arbitrary(nonEmptyAlphaStrGen)
    forall { (keys: List[String], vs: (Row, Row)) =>
      val values = List(("l", vs._1), ("r", vs._2))
      val res    = getDeltaInfo(keys, values)
      res match {
        case DeltaInfo(keys, Nil, _: DiffType.Incomparable) => success
        case _                                              =>
          failure(s"DeltaInfo should be DeltaInfo($keys, Nil, DiffType.Incomparable), got $res")
      }
    }
  }

  test("getDeltaInfo - same row identified as l/r") {
    implicit val rowArb: Arbitrary[Row] =
      Arbitrary(structTypeGen(simpleStructFieldGen).flatMap(_._2))
    implicit val stringArb: Arbitrary[String] = Arbitrary(nonEmptyAlphaStrGen)
    forall { (keys: List[String], vs: Row) =>
      val values = List(("l", vs), ("r", vs))
      val res    = getDeltaInfo(keys, values)
      expect.eql(DeltaInfo(keys, Nil, DiffType.Same), res)
    }
  }

  test("getDeltaInfo - 2 rows unidentified as l/r") {
    implicit val stringArb: Arbitrary[String] = Arbitrary(nonEmptyAlphaStrGen)
    forall { (keys: List[String], vs: ((String, Row), (String, Row))) =>
      val values = List(vs._1, vs._2)
      val res    = getDeltaInfo(keys, values)
      val msg    = s"Could not identify lhs and rhs for key: ${keys.mkString(", ")}"
      expect.eql(DeltaInfo(keys, Nil, DiffType.Incomparable(msg)), res)
    }
  }

  test("getDeltaInfo - less than 2 rows") {
    forall { (keys: List[String], vs: (String, Row)) =>
      val values = List(vs)
      val res    = getDeltaInfo(keys, values)
      expect.eql(DeltaInfo(keys, Nil, DiffType.MissingLHS), res)
    }
  }

  test("getDeltaInfo - more than 2 rows") {
    forall { (keys: List[String], vs: (Row, Row, Row)) =>
      val values = List(("a", vs._1), ("b", vs._2), ("c", vs._3))
      val res    = getDeltaInfo(keys, values)
      val msg    =
        s"More than 2 values found for key: ${keys.mkString(", ")}, it should be a primary key"
      expect.eql(DeltaInfo(keys, Nil, DiffType.Incomparable(msg)), res)
    }
  }

  test("initStats") {
    forall { (ds: List[Delta], dt: DiffType) =>
      val (vec, map) = initStats(ds, dt)
      (dt match {
        case DiffType.Same            => expect.eql(1L, vec._2)
        case DiffType.Different       => expect.eql(1L, vec._3)
        case DiffType.MissingLHS      => expect.eql(1L, vec._4)
        case DiffType.MissingRHS      => expect.eql(1L, vec._5)
        case _: DiffType.Incomparable => expect.eql(1L, vec._6)
      }) &&
      expect.eql(1L, vec._1) &&
      expect.eql(map.size, ds.map(_.fieldName).toSet.size) &&
      expect(map.values.forall(_._1 == 1L))
    }
  }

  test("finalizeStats") {
    forall { (vec: (Long, Long, Long, Long, Long, Long), fieldMap: Map[String, MapVal]) =>
      val res = finalizeStats(vec, fieldMap)
      expect.eql((GlobalStats.apply _).tupled(vec), res.global) &&
      expect.eql(fieldMap.size, res.fields.size)
    }
  }

  test("keyFn") {
    forall { st: (StructType, Gen[Row]) =>
      val fields = st._1.fields.map(_.name)
      val row    = st._2.sample
      val res    = row.map(keyFn(fields.toSet)(_))
      expect.eql(Some(fields.size), res.map(_.size)) &&
      expect.eql(row.map(_.toSeq.map(_.toString).toSet), res.map(_.toSet))
    }
  }
}
