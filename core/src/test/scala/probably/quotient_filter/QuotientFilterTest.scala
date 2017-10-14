package probably.quotient_filter

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scalaprops.{Choose, Gen, Property, Scalaprops}

object QuotientFilterTest extends Scalaprops with Gens {

  val minInputs = 100
  val maxInputs = 1000

  override def param = super.param.copy(
    minSuccessful = 1000,
    timeout = 5 minute
  )

  override def rBitsRange = 10 to 20

  override def qBitsRange = 10 to 15

  def inputsGenWithCapacity(min: Int = minInputs, max: Int = maxInputs) = {
    def numInputs(cfg: Configs) = scala.math.min(
      scala.math.min(
        cfg.capacity.toInt,
        scala.util.Random.nextInt(cfg.capacity.toInt - min) + min
      ),
      max
    )

    for {
      cfg <- configsGen
      strs <- Gen.sequenceNList(numInputs(cfg), Gen.asciiString)
    } yield (cfg, strs)
  }

  val addExceedingCapacity = Property.forAllG(
    for {
      cfg <- for {
        rBits <- Choose.intChoose.choose(10, 20)
        qBits <- Choose.intChoose.choose(4, 10)
      } yield Configs(qBits = qBits, rBits = rBits)
      strs <- Gen.sequenceNList(cfg.capacity.toInt * 2,
                                Gen.genString(Gen.asciiChar, 10))
    } yield (cfg, strs)
  ) {
    case (cfg, strs) =>
      implicit val ser = (s: String) => s.getBytes
      val qf = QuotientFilter[String](cfg)
      val h: String => Long = s => qf.hash(ser(s))

      // avoid hard collision
      val inputs = strs
        .groupBy(s => qf.quotientAndReminder(h(s)))
        .mapValues(_.head)
        .values
        .toList
      if (inputs.length <= cfg.capacity) {
        println(
          s"WARNING: inputs(length = ${inputs.length}, capacity = ${cfg.capacity}) are NOT large enough.")
      }

      Try(inputs.foreach(qf.insert)) match {
        case Failure(th) => th == CapacityOverflowedError(cfg.capacity)
        case Success(_) => false
      }
  }

  val addAndRemoveWithinCapacityAndHardCollision =
    Property.forAllG(inputsGenWithCapacity()) {
      case (cfg, inputs) =>
        // happen hard collision all the time
        implicit val ser = (s: String) => "abcd".getBytes
        val qf = QuotientFilter[String](cfg)
        val h: String => Long = s => qf.hash(ser(s))

        val properlyInserted: Seq[Boolean] = inputs.zipWithIndex.map {
          case (s, i) =>
            qf.insert(s)
            qf.maybeContains(s) && (qf.nEntries == i + 1)
        }

        val properlyRemoved: Seq[Boolean] = inputs.zipWithIndex.map {
          case (s, i) =>
            qf.remove(s)
            // contains should be false only after the last remove, others should be true.
            val contains =
              if (i != inputs.length - 1) qf.maybeContains(s)
              else !qf.maybeContains(s)
            contains && (qf.nEntries == (inputs.length - 1 - i))
        }

        val result =
          properlyInserted.zip(properlyRemoved).forall(b => b._1 && b._2)
        if (!result) {
          println(s"configs = ${cfg}")
          println(
            s"inputs(length = ${inputs.length}): " + inputs.mkString(","))
          println(
            s"properlyInserted(length = ${properlyInserted.length}): " + properlyInserted
              .mkString(","))
          println(
            s"properlyRemove(length = ${properlyRemoved.length}): " + properlyRemoved
              .mkString(","))
          println(s"table = ${qf.T}")
          println(s"last nEntries = ${qf.nEntries}")
        }
        result
    }

  val addAndRemoveWithinCapacityAndNoHardCollision =
    Property.forAllG(inputsGenWithCapacity()) {
      case (cfg, strs) =>
        implicit val ser = (s: String) => s.getBytes
        val qf = QuotientFilter[String](cfg)
        val h: String => Long = s => qf.hash(ser(s))

        // avoid hard collision
        val inputs = strs
          .groupBy(s => qf.quotientAndReminder(h(s)))
          .mapValues(_.head)
          .values
          .toList

        val properlyInserted: Seq[Boolean] = inputs.zipWithIndex.map {
          case (s, i) =>
            qf.insert(s)
            qf.maybeContains(s) && (qf.nEntries == i + 1)
        }
        val properlyRemoved: Seq[Boolean] = inputs.zipWithIndex.map {
          case (s, i) =>
            qf.remove(s)
            !qf.maybeContains(s) && (qf.nEntries == (inputs.length - 1 - i))
        }

        val result =
          properlyInserted.zip(properlyRemoved).forall(b => b._1 && b._2)
        if (!result) {
          println(s"configs = ${cfg}")
          println(
            s"inputs(length = ${inputs.length}): " + inputs.mkString(","))
          println(
            s"properlyInserted(length = ${properlyInserted.length}): " + properlyInserted
              .mkString(","))
          println(
            s"properlyRemove(length = ${properlyRemoved.length}): " + properlyRemoved
              .mkString(","))
          println(s"table = ${qf.T}")
          println(s"last nEntries = ${qf.nEntries}")
        }
        result
    }

}
