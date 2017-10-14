package probably.quotient_filter

import org.scalatest.Inside

import scalaprops.{Gen, Param, Property, Scalaprops}

object SlotTest extends Scalaprops with Gens {

  override def param: Param =
    Param.withCurrentTimeSeed().copy(minSuccessful = 1000)

  val rBitsRange = 3 to 15
  val qBitsRange = 5 to 20

  val settersWorksProperly = Property.forAllG(configsGen, Gen.positiveLong) {
    (cfg, reminder) =>
      implicit val _cfg = cfg
      import cfg._
      val o = Slot.empty.setOccupied match {
        case Slot(true, false, false, 0L) => true
        case _ => false
      }

      val c = Slot.empty.setContinuation match {
        case Slot(false, true, false, 0L) => true
        case _ => false
      }

      val s = Slot.empty.setShifted match {
        case Slot(false, false, true, 0L) => true
        case _ => false
      }

      val r = Slot.empty.setReminder(reminder) match {
        case Slot(false, false, false, rem) if rem == (reminder & rMask) =>
          true
        case _ => false
      }

      o && c && s && r
  }

  val constructorShouldPreserveArgument = Property
    .forAllG(configsGen,
             Gen.genBoolean,
             Gen.genBoolean,
             Gen.genBoolean,
             Gen.genLongAll) {
      (cfg, isOccupied, isContinuation, isSkipped, reminderRaw) =>
        import cfg._
        val reminder = reminderRaw & rMask
        val slot = Slot(isOccupied, isContinuation, isSkipped, reminder)(cfg)
        val extractorWorks = {
          val Slot(o, c, s, r) = slot
          (o == isOccupied
          && c == isContinuation
          && s == isSkipped
          && r == reminder)
        }
        val accessorWorks = (slot.isOccupied == isOccupied
          && slot.isContinuation == isContinuation
          && slot.isShifted == isSkipped
          && slot.reminder == reminder)

        extractorWorks && accessorWorks
    }

  val updaterShouldSetCorrectValues =
    Property.forAllG(slotGen, Gen.genLongAll) {
      case ((cfg, slot), newReminderRaw) =>
        import cfg._
        val newReminder = newReminderRaw & rMask

        (slot.setOccupied.isOccupied
        && slot.setContinuation.isContinuation
        && slot.setShifted.isShifted
        && slot.setReminder(newReminder).reminder == newReminder
        && !slot.clearOccupied.isOccupied
        && !slot.clearContinuation.isContinuation
        && !slot.clearShifted.isShifted
        && slot.clearReminder.reminder == 0L
        && slot.clear.bits == 0L)
    }
}
