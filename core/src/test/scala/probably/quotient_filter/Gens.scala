package probably.quotient_filter

import scalaprops.{Choose, Gen}

trait Gens {
  def rBitsRange: Range
  def qBitsRange: Range

  lazy val rBitsGen = Choose.intChoose.choose(rBitsRange.min, rBitsRange.max)
  lazy val qBitsGen = Choose.intChoose.choose(qBitsRange.min, qBitsRange.max)

  lazy val configsGen = for {
    rBits <- rBitsGen
    qBits <- qBitsGen
  } yield Configs(qBits = qBits, rBits = rBits)

  lazy val slotGen = for {
    cfg <- configsGen
    isOccupied <- Gen.genBoolean
    isContinuation <- Gen.genBoolean
    isSkipped <- Gen.genBoolean
    reminderRaw <- Gen.genLongAll
  } yield {
    import cfg._
    cfg -> Slot(isOccupied, isContinuation, isSkipped, reminderRaw & rMask)(
      cfg)
  }

  def slotGen(cfg: Configs) =
    for {
      isOccupied <- Gen.genBoolean
      isContinuation <- Gen.genBoolean
      isSkipped <- Gen.genBoolean
      reminderRaw <- Gen.genLongAll
    } yield {
      import cfg._
      Slot(isOccupied, isContinuation, isSkipped, reminderRaw & rMask)(cfg)
    }

  def qfGen =
    for {
      cfg <- configsGen
      size <- Choose.intChoose.choose(100, 1000)
      slots <- Gen.sequenceNList(size, slotGen(cfg))
      idxs <- Gen.sequenceNList(size, Gen.positiveLong)
    } yield {
      val t = CompactQFTable()(cfg)
      slots.zip(idxs).foreach {
        case (slot, idx) =>
          t.update(idx, slot)
      }
      cfg -> t
    }
}
