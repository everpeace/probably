package probably.quotient_filter

case class CapacityOverflowedError(capacity: Long)
    extends AssertionError(s"capacity overflowed (capacity =${capacity})")
