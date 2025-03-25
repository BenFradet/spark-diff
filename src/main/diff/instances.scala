package diff

import cats.Eq
import cats.instances.list._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import com.twitter.algebird.Monoid
import org.apache.spark.SparkContext
import com.twitter.algebird.Semigroup

object instances {
  implicit val eqStructField: Eq[StructField] = Eq.fromUniversalEquals
  implicit val eqStructType: Eq[StructType] = new Eq[StructType] {
    override def eqv(x: StructType, y: StructType): Boolean =
      Eq.eqv[List[StructField]](x.fields.toList.sortBy(_.name), y.fields.toList.sortBy(_.name))
  }

  implicit class ToAlgebird[T](val rdd: RDD[T]) extends AnyVal {
    def algebird: AlgebirdRDD[T] = new AlgebirdRDD[T](rdd)
  }

  def rddMonoid[T: ClassTag](sc: SparkContext): Monoid[RDD[T]] = new Monoid[RDD[T]] {
    def zero = sc.emptyRDD[T]
    override def isNonZero(s: RDD[T]) = s.isEmpty()
    def plus(a: RDD[T], b: RDD[T]) = a.union(b)
  }

  implicit def rddSemigroup[T]: Semigroup[RDD[T]] = new Semigroup[RDD[T]] {
    def plus(a: RDD[T], b: RDD[T]) = a.union(b)
  }
}

