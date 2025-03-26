package diff

import cats.instances.char._
import cats.syntax.eq._
import cats.syntax.option._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalacheck.Gen
import weaver._
import weaver.scalacheck._

import gen._
import cats.Show

object DeltaSpec extends SimpleIOSuite with Checkers {
  implicit val showRow: Show[Row]               = Show.fromToString
  implicit val showGenRow: Show[Gen[Row]]       = Show.fromToString
  implicit val showStructType: Show[StructType] = Show.fromToString

  test("Delta.diff - different schemas") {
    forall { (row1: Row, row2: Row) =>
      val res = Delta.diff(row1, row2)
      expect(res.isLeft)
    }
  }

  test("Delta.diff - same row") {
    forall { row: Row =>
      val res = Delta.diff(row, row).map(_.map(_.deltaValue))
      expect(res.isRight) &&
      expect(res.getOrElse(Nil).forall {
        case DeltaValue.UnsupportedDelta(tipe) => tipe.startsWith("ArrayType")
        case DeltaValue.TypedDelta(_, value)   => value === 0d
      })
    }
  }

  test("Delta.diff - diferrent rows same schema") {
    forall { st: (StructType, Gen[Row]) =>
      val gen = st._2
      (gen.sample, gen.sample) match {
        case (Some(r1), Some(r2)) =>
          val res = Delta.diff(r1, r2)
          expect(res.isRight) && expect(
            res.map(_.map(_.deltaValue)).getOrElse(Nil).forall {
              case DeltaValue.UnsupportedDelta(tipe) => tipe.startsWith("ArrayType")
              case _: DeltaValue.TypedDelta          => true
            }
          )
        case _ => success // we don't care about other cases and sample shouldn't fail
      }
    }
  }

  test("Delta.doubleValue - byte") {
    forall { byte: Option[Byte] =>
      val res = Delta.doubleValue(byte)
      byte match {
        case Some(_) => expect.eql(byte.map(_.toDouble), res)
        case None    => expect.eql(None, res)
      }
    }
  }

  test("Delta.doubleValue - double") {
    forall { double: Option[Double] =>
      val res = Delta.doubleValue(double)
      expect.eql(double, res)
    }
  }

  test("Delta.doubleValue - float") {
    forall { float: Option[Float] =>
      val res = Delta.doubleValue(float)
      float match {
        case Some(_) => expect.eql(float, res.map(_.toFloat))
        case None    => expect.eql(None, res)
      }
    }
  }

  test("Delta.doubleValue - int") {
    forall { int: Option[Int] =>
      val res = Delta.doubleValue(int)
      int match {
        case Some(_) => expect.eql(int.map(_.toDouble), res)
        case None    => expect.eql(None, res)
      }
    }
  }

  test("Delta.doubleValue - long") {
    forall { long: Option[Long] =>
      val res = Delta.doubleValue(long)
      long match {
        case Some(_) => expect.eql(long.map(_.toDouble), res)
        case None    => expect.eql(None, res)
      }
    }
  }

  test("Delta.doubleValue - big decimal") {
    forall { double: Option[Double] =>
      val bd  = double.map(new java.math.BigDecimal(_))
      val res = Delta.doubleValue(bd)
      bd match {
        case Some(_) => expect.eql(bd.map(_.doubleValue()), res)
        case None    => expect.eql(None, res)
      }
    }
  }

  test("Delta.booleanValue") {
    forall { boolean: Option[Boolean] =>
      val res = Delta.booleanValue(boolean)
      boolean match {
        case Some(true)  => expect.eql(Some(1d), res)
        case Some(false) => expect.eql(Some(0d), res)
        case None        => expect.eql(None, res)
      }
    }
  }

  test("Delta.stringValue - string") {
    forall { string: Option[String] =>
      val res = Delta.stringValue(string)
      expect.eql(string, res)
    }
  }

  test("Delta.stringValue - date") {
    forall { long: Option[Long] =>
      val date = long.map(new java.sql.Date(_))
      val res  = Delta.stringValue(date)
      expect.eql(date.map(_.toString()), res)
    }
  }

  test("Delta.stringValue - timestamp") {
    forall { long: Option[Long] =>
      val ts  = long.map(new java.sql.Timestamp(_))
      val res = Delta.stringValue(ts)
      expect.eql(ts.map(_.toString()), res)
    }
  }

  test("Delta.getValue - missing field name") {
    forall { (key: String, value: String) =>
      val row = Row(value)
      val res = Delta.getValue(row, key)
      expect(None == res)
    }
  }

  test("Delta.getValue - null value") {
    forall { (key: String) =>
      val row =
        new GenericRowWithSchema(Array(null), StructType(Array(StructField(key, StringType, true))))
      val res = Delta.getValue(row, key)
      expect(None == res)
    }
  }

  test("Delta.getValue - existing field name") {
    forall { (key: String, value: String) =>
      val row =
        new GenericRowWithSchema(
          Array(value),
          StructType(Array(StructField(key, StringType, true)))
        )
      val res = Delta.getValue(row, key)
      expect(res == Some(value))
    }
  }

  test("Delta.stringDelta - levenshtein - empty left/right") {
    forall { s: String =>
      val resL = Delta.stringDelta("", s)
      val resR = Delta.stringDelta(s, "")
      expect.eql(s.length().toDouble, resL) && expect.eql(s.length().toDouble, resR)
    }
  }

  // taken from https://en.wikipedia.org/wiki/Levenshtein_distance
  test("Delta.stringDelta - levenshtein properties") {
    forall { (s1: String, s2: String) =>
      val res = Delta.stringDelta(s1, s2)
      val s1l = s1.length()
      val s2l = s2.length()
      expect(res >= (s1l - s2l).abs) && expect(res <= math.max(s1l, s2l).toDouble)
    }
  }

  test("Delta.stringDelta - levenshtein - same string") {
    forall { s: String =>
      val res = Delta.stringDelta(s, s)
      expect.eql(0d, res)
    }
  }

  test("Delta.stringDelta - levenshtein - triangle inequality") {
    forall { (s1: String, s2: String, s3: String) =>
      val res12 = Delta.stringDelta(s1, s2)
      val res13 = Delta.stringDelta(s1, s3)
      val res23 = Delta.stringDelta(s2, s3)
      expect(res12 <= res13 + res23)
    }
  }

  def hammingDistance(s1: String, s2: String): Double =
    s1.zip(s2).foldLeft(0d) { case (acc, (l, r)) =>
      if (l =!= r) acc + 1d
      else acc
    }

  test("Delta.stringDelta - levenshtein - hamming distance") {
    val sameSizeStrGen = Gen.listOfN(20, Gen.alphaChar).map(_.mkString)
    forall(Gen.zip(sameSizeStrGen, sameSizeStrGen)) { (t: (String, String)) =>
      val res = Delta.stringDelta(t._1, t._2)
      expect(res <= hammingDistance(t._1, t._2))
    }
  }

  test("Delta.optionDelta - g") {
    forall { (d1: Double, d2: Double) =>
      val res = Delta.optionDelta[Double](d1.some, d2.some, identity, _ + _)
      expect.eql(d1 + d2, res)
    }
  }

  test("Delta.optionDelta - f") {
    forall { d: Double =>
      val res1 = Delta.optionDelta[Double](d.some, None, identity, _ + _)
      val res2 = Delta.optionDelta[Double](None, d.some, identity, _ + _)
      expect.eql(d, res1) && expect.eql(d, res2)
    }
  }
}
