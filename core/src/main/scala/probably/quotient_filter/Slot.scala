package probably.quotient_filter

import probably.quotient_filter.Slot._

/**
  * data structure of entries in QF[fq]
  * This hold data of (num of remider bits + 3) bits by Long
  * the lowest three bits is used for metadata(occupied, continuation, shifted)
  */
protected class Slot private (val bits: Long)(implicit cfg: Configs) {

  import cfg._

  def element: Long = bits & sMask

  def isOccupied: Boolean = (element & occupiedMask) != 0

  def isContinuation: Boolean = (element & continuationMask) != 0

  def isShifted: Boolean = (element & shiftedMask) != 0

  def isEmpty: Boolean = (element & 7) == 0

  def isRunStart: Boolean = !isContinuation & (isOccupied | isShifted)

  def isClusterStart: Boolean = isOccupied & !isContinuation & !isShifted

  def reminder: Long = (element >>> 3) & rMask

  def setOccupied: Slot = Slot(element | occupiedMask)

  def setContinuation: Slot = Slot(element | continuationMask)

  def setShifted: Slot = Slot(element | shiftedMask)

  def setReminder(reminder: Long): Slot = {
    val meta = element & 7 // maskLower(3) = 7
    val r = ((reminder & rMask) << 3) & ~7
    Slot(r | meta)
  }

  def clearOccupied: Slot = Slot(element & ~occupiedMask)

  def clearContinuation: Slot = Slot(element & ~continuationMask)

  def clearShifted: Slot = Slot(element & ~shiftedMask)

  def clearReminder: Slot = setReminder(0L)

  def clear: Slot = Slot(0L)

  override def toString() = {
    val Slot(o, c, s, r) = this
    s"Slot($o, $c, $s, $r)"
  }

  def canEqual(a: Any) = a.isInstanceOf[Slot]

  override def equals(that: Any): Boolean =
    that match {
      case that: Slot => that.canEqual(this) && this.element == that.element
      case _ => false
    }
}

protected object Slot {
  val occupiedMask = 1L << 2
  val continuationMask = 1L << 1
  val shiftedMask = 1L

  def apply(bits: Long = 0L)(implicit cfg: Configs): Slot = new Slot(bits)

  def empty(implicit ctg: Configs) = Slot()

  def apply(isOccupied: Boolean,
            isContinuation: Boolean,
            isShifted: Boolean,
            reminder: Long)(implicit stg: Configs): Slot = {
    val init = Slot(0L)
    val o = if (isOccupied) init.setOccupied else init
    val c = if (isContinuation) o.setContinuation else o
    val s = if (isShifted) c.setShifted else c
    s.setReminder(reminder)
  }

  def unapply(arg: Slot): Option[(Boolean, Boolean, Boolean, Long)] =
    Option((arg.isOccupied, arg.isContinuation, arg.isShifted, arg.reminder))
}
