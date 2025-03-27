package diff

import com.twitter.algebird.{Max, Min, Moments}

object types {

  /** Type alias used internally when computing field statistics. */
  type MapVal = (Long, Option[(DeltaType, Min[Double], Max[Double], Moments)])
}
