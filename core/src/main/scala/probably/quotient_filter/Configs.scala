package probably.quotient_filter

import probably.util._

case class Configs(
    qBits: Int,
    rBits: Int,
    maxInsertionRatio: Double = 0.75
) {
  import Configs._
  require(qBits > 0, "qBits(quotient bit length) must be positive.")
  require(qBits <= QBITS_MAX,
          s"qBits(quotient bit length) must be leq $QBITS_MAX")
  require(rBits > 0, "rBits(reminder bit length) must be positive.")
  require(rBits <= RBITS_MAX,
          s"rBits(reminder bit length) must be leq $RBITS_MAX")
  require(
    qBits + rBits <= 64,
    s"qBits(quotient bit length) + rBits(reminder bit length) must be leq 64")

  val slotBits: Int = rBits + 3
  val qMax: Long = 1L << qBits
  val capacity = qMax
  val maxInsertion = qMax * maxInsertionRatio
  val compatQFTableSize: Int = {
    val bits = (1L << qBits) * slotBits
    val longs = bits / 64
    val ret = if ((bits % 64) > 0) longs + 1 else longs
    require(
      ret <= Int.MaxValue,
      s"table size must be leq Int.MaxValue(${Int.MaxValue}). you should set smaller quotient/reminder bit length.")
    ret.toInt
  }

  val qMask: Long = maskLower(qBits)
  val rMask: Long = maskLower(rBits)
  val sMask: Long = maskLower(slotBits)
}

object Configs {
  final val QBITS_MAX = 32 // bit length of Int
  final val RBITS_MAX = 64 - 3 // slot should be kept with long value.
}
