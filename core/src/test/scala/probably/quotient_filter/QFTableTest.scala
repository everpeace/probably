package probably.quotient_filter

import scalaprops._

object QFTableTest extends Scalaprops with Gens {
  override def param: Param =
    Param.withCurrentTimeSeed().copy(minSuccessful = 1000)

  val rBitsRange = 3 to 15
  val qBitsRange = 5 to 20

  val putAndThenGetTheValue = Property.forAllG(slotGen, Gen.positiveLong) {
    case ((cfg, slot), idx) =>
      implicit val _cfg = cfg
      val t = CompactQFTable()
      t.update(idx, slot)
      t.get(idx).equals(slot)
  }

  val getAndPutShouldIdentity = Property.forAllG(qfGen, Gen.positiveLong) {
    case ((_, qfTable), idx) =>
      val oldTable = Array.ofDim[Long](qfTable.underlying.length)
      qfTable.underlying.copyToArray(oldTable)

      // get and put
      qfTable.update(idx, qfTable.get(idx))

      // get and put don't change the table
      oldTable.deep == qfTable.underlying.deep
  }

  val multipleSamePutsShouldEqualToSinglePut = Property.forAllG(for {
    cfg_qf <- qfGen
    (cfg, qf) = cfg_qf
    idx <- Gen.positiveLong
    slot1 <- slotGen(cfg)
    slot2 <- slotGen(cfg)
  } yield (cfg, qf, idx, slot1, slot2)) { tup =>
    val (cfg, qf, idx, slot1, slot2) = tup

    // put(slot1) and then put(slot2)
    val qf1Table = Array.ofDim[Long](qf.underlying.length)
    qf.underlying.copyToArray(qf1Table)
    val qf1 = CompactQFTable(qf1Table)(cfg)
    qf1.update(idx, slot1)
    qf1.update(idx, slot2)

    // just put(slot2)
    val qf2Table = Array.ofDim[Long](qf.underlying.length)
    qf.underlying.copyToArray(qf2Table)
    val qf2 = CompactQFTable(qf2Table)(cfg)
    qf2.update(idx, slot2)

    // put(slot1),put(slot2) == put(slot2)
    qf1Table.deep == qf2Table.deep
  }

}
