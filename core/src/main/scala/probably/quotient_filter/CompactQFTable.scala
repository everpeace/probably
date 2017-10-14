package probably.quotient_filter
import probably.util._

/**
  * This is a compact implementation of QF[fq] table by Array[Long].
  */
protected class CompactQFTable(val underlying: Array[Long])(
    implicit cfg: Configs)
    extends (Long => Slot) {
  import cfg._
  require(underlying.size == compatQFTableSize)

  override def apply(idx: Long) = get(idx)

  /**
    * @param fq
    * @return QF[fq]
    */
  def get(fq: Long): Slot = {
    val wrappedIdx = fq & qMask
    val bitPos = wrappedIdx * slotBits
    val tabIdx = (bitPos / 64).toInt
    val slotPos = (bitPos % 64).toInt
    val spillBits = (slotPos + slotBits) - 64
    val elm = underlying(tabIdx) >>> slotPos
    if (spillBits > 0) {
      val spill = (underlying(tabIdx + 1) & maskLower(spillBits)) << (slotBits - spillBits)
      Slot((elm | spill) & sMask)
    } else {
      Slot(elm & sMask)
    }
  }

  /**
    * set slot s to QF[fq]
    * note: you can write QF(fq) = slot because this is a syntax sugar of QF.update(fq, slot)
    * @param fq
    * @param s slot to assign
    */
  def update(fq: Long, s: Slot): Unit = {
    val wrappedIdx = fq & qMask
    val bitPos = wrappedIdx * slotBits
    val tabIdx = (bitPos / 64).toInt
    val slotPos = (bitPos % 64).toInt
    val spillBits = (slotPos + slotBits) - 64
    val elm = s.element
    val oldCell = underlying(tabIdx)
    val newCell = (oldCell & ~(sMask << slotPos)) | (elm << slotPos)
    underlying(tabIdx) = newCell
    if (spillBits > 0) {
      val oldSpillCell = underlying(tabIdx + 1)
      val newSpillCell = oldSpillCell & ~maskLower(spillBits) | (elm >>> (slotBits - spillBits))
      underlying(tabIdx + 1) = newSpillCell
    }
  }

  override def toString() = {
    (0L until qMax)
      .map(get)
      .zipWithIndex
      .map { case (s, i) => s"$i = $s" }
      .mkString("\n")
  }
}

protected object CompactQFTable {
  def apply(table: Array[Long])(implicit cfg: Configs) =
    new CompactQFTable(table)
  def apply()(implicit cfg: Configs) =
    new CompactQFTable(Array.ofDim(cfg.compatQFTableSize))
}
