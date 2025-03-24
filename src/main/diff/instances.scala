package diff

import cats.Eq
import cats.instances.list._
import org.apache.spark.sql.types.{StructField, StructType}

object instances {
  implicit val eqStructField: Eq[StructField] = Eq.fromUniversalEquals
  implicit val eqStructType: Eq[StructType] = new Eq[StructType] {
    override def eqv(x: StructType, y: StructType): Boolean =
      Eq.eqv[List[StructField]](x.fields.toList.sortBy(_.name), y.fields.toList.sortBy(_.name))
  }
}

