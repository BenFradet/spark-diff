package diff

import java.sql.{Date, Timestamp}

import com.twitter.algebird._
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}

import types.MapVal
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.Row

object gen {
  val nonEmptyAlphaStrGen: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)

  val momentsArb: Arbitrary[Moments] = Arbitrary {
    for {
      m0 <- Gen.choose(1L, Int.MaxValue.toLong)
      m1 <- Gen.choose(-1e50, 1e50)
      m2 <- Gen.choose(0, 1e50)
      m3 <- Gen.choose(-1e10, 1e50)
      m4 <- Gen.choose(0, 1e50)
    } yield new Moments(m0, m1, m2, m3, m4)
  }

  def minArb[T: Arbitrary]: Arbitrary[Min[T]] =
    Arbitrary(Arbitrary.arbitrary[T].map(Min(_)))

  def maxArb[T: Arbitrary]: Arbitrary[Max[T]] =
    Arbitrary(Arbitrary.arbitrary[T].map(Max(_)))

  val deltaTypeGen: Gen[DeltaType] = Gen.oneOf(DeltaType.Numeric, DeltaType.String)

  val mapValGen: Gen[MapVal] =
    for {
      l       <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      dt      <- deltaTypeGen
      min     <- minArb[Double].arbitrary
      max     <- maxArb[Double].arbitrary
      moments <- momentsArb.arbitrary
      all     <- Gen.zip(dt, min, max, moments)
      option  <- Gen.option(all)
    } yield (l, option)

  implicit val mapValArb: Arbitrary[MapVal] = Arbitrary(mapValGen)

  implicit val diffTypeArb: Arbitrary[DiffType] = Arbitrary {
    for {
      msg <- Gen.alphaLowerStr
      dt <- Gen.oneOf(
        DiffType.Same,
        DiffType.Different,
        DiffType.MissingLHS,
        DiffType.MissingRHS,
        DiffType.Incomparable(msg)
      )
    } yield dt
  }

  val deltaValueGen: Gen[DeltaValue] = for {
    sparkType <- nonEmptyAlphaStrGen
    dt        <- deltaTypeGen
    d         <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
    dv        <- Gen.oneOf(DeltaValue.TypedDelta(dt, d), DeltaValue.UnsupportedDelta(sparkType))
  } yield dv

  implicit val deltaArb: Arbitrary[Delta] = Arbitrary {
    for {
      fieldName <- nonEmptyAlphaStrGen
      v1        <- Gen.option(nonEmptyAlphaStrGen)
      v2        <- Gen.option(nonEmptyAlphaStrGen)
      value     <- deltaValueGen
    } yield Delta(fieldName, v1, v2, value)
  }

  val tsGen: Gen[Timestamp] =
    for {
      y <- Gen.chooseNum(1980, 2030)
      m <- Gen.chooseNum(1, 12)
      d <- Gen.chooseNum(1, 29)
    } yield Timestamp.valueOf(s"$y-$m-$d 00:00:00")

  val simpleDataTypeGen: Gen[(DataType, Gen[Any])] = Gen.oneOf(
    (BooleanType, Gen.oneOf(true, false)),
    (ByteType, Gen.chooseNum(Byte.MinValue, Byte.MaxValue)),
    (DateType, Gen.chooseNum(Int.MinValue, Int.MaxValue).map(i => new Date(i.toLong))),
    (DoubleType, Gen.chooseNum(Double.MinValue, Double.MaxValue)),
    (FloatType, Gen.chooseNum(Float.MinValue, Float.MaxValue)),
    (IntegerType, Gen.chooseNum(Int.MinValue, Int.MaxValue)),
    (LongType, Gen.chooseNum(Long.MinValue, Long.MaxValue)),
    (ShortType, Gen.chooseNum(Short.MinValue, Short.MaxValue)),
    (StringType, Gen.alphaNumStr),
    (TimestampType, tsGen)
  )

  // without array types as they are not comparable
  val simpleStructFieldGen: Gen[(StructField, Gen[Any])] =
    for {
      name <- nonEmptyAlphaStrGen
      dt   <- simpleDataTypeGen
    } yield (StructField(name, dt._1), dt._2)

  // we don't generate map data types because columns with this type are not hashable so drop
  // duplicates won't work
  val complexDataTypeGen: Gen[(DataType, Gen[Any])] =
    for {
      (dt, g) <- simpleDataTypeGen
      d = (ArrayType(dt, true), Gen.listOfN(20, g).map(_.toArray))
    } yield d

  // we don't bother with nested struct types as we don't have them
  val structFieldGen: Gen[(StructField, Gen[Any])] =
    for {
      name <- nonEmptyAlphaStrGen
      dt   <- Gen.frequency((9, simpleDataTypeGen), (1, complexDataTypeGen))
    } yield (StructField(name, dt._1), dt._2)

  implicit val structFieldArb: Arbitrary[List[StructField]] =
    Arbitrary(Gen.nonEmptyListOf(structFieldGen.map(_._1)))

  def normalizeFieldName(tuple: (StructField, Gen[Any])): (StructField, Gen[Any]) = tuple match {
    case ((sf, g)) => (sf.copy(name = sf.name.toLowerCase()), g)
  }

  def structTypeGen(structFieldGen: Gen[(StructField, Gen[Any])]): Gen[(StructType, Gen[Row])] =
    for {
      l <- Gen.nonEmptyListOf(structFieldGen).map {
        _.take(20)
          .map(normalizeFieldName)
          .groupBy(_._1.name)
          .map(_._2.head) // filter out any duplicate column names
          .toMap
      }
      st = StructType(l.keys.toSeq)
    } yield (st, Gen.sequence[Array[Any], Any](l.values).map(new GenericRowWithSchema(_, st)))

  implicit val structTypeArb: Arbitrary[(StructType, Gen[Row])] = Arbitrary(
    structTypeGen(structFieldGen)
  )

  implicit val rowArb: Arbitrary[Row] = Arbitrary(structTypeGen(structFieldGen).flatMap(_._2))
}
