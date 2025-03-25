package diff

import com.twitter.algebird.Min
import com.twitter.algebird.Max
import com.twitter.algebird.Moments

object types {
  /** Type alias used internally when computing field statistics. */
  type MapVal = (Long, Option[(DeltaType, Min[Double], Max[Double], Moments)])
}
