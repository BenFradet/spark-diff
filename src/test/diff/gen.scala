package diff

import java.sql.{Date, Timestamp}

import com.twitter.algebird._
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}

import types.MapVal

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
}
