package probably.quotient_filter

import probably.hashing.FNV

import scala.annotation.tailrec

/**
  * Quotient Filter implementation.
  * reference:
  * Michael A. Bender et.al. "Don't thrash: how to cache your hash on flash".
  * The Proceedings of the VLDB Endowment (PVLDB). 5 (11): 1627–1637.
  * http://vldb.org/pvldb/vol5/p1627_michaelabender_vldb2012.pdf
  */
class QuotientFilter[E] private (
    val cfg: Configs,
    val T: CompactQFTable,
    val hash: Array[Byte] => Long
)(implicit val serializer: E => Array[Byte]) {

  import cfg._

  implicit val _cfg = cfg

  private var _nEntries: Long = 0

  def nEntries = _nEntries

  private def incrementIdx(s: Long) = (s + 1) & qMask

  private def decrementIdx(s: Long) = (s - 1) & qMask

  def quotientAndReminder(h: Long): (Long, Long) =
    ((h >>> rBits) & qMask) -> (h & rMask)

  @tailrec
  private def findNextIdx(startExclusive: Long,
                          predicate: Slot => Boolean): Long = {
    val next = incrementIdx(startExclusive)
    if (predicate(T(next))) {
      next
    } else {
      findNextIdx(next, predicate)
    }
  }

  @tailrec
  private def findStartOfCluster(b: Long): Long =
    if (!T(b).isShifted) {
      b
    } else {
      findStartOfCluster(decrementIdx(b))
    }

  @tailrec
  private def findTheStartOfTheRunForFq(b: Long,
                                        s: Long,
                                        fq: Long): (Long, Long, Long) =
    if (b == fq) {
      (b, s, fq)
    } else {
      val _s = findNextIdx(s, !_.isContinuation)
      val _b = findNextIdx(b, _.isOccupied)
      findTheStartOfTheRunForFq(_b, _s, fq)
    }

  /**
    * Find the start index of the run for fq (given that the run exists).
    */
  private def findRunIndex(fq: Long): Long = {
    val b = findStartOfCluster(fq)
    val (_, s, _) = findTheStartOfTheRunForFq(b, b, fq)
    s
  }

  /* Insert slot into QF[s], shifting over elements as necessary. */
  private def insertInto(s: Long, slot: Slot): Unit = {
    @tailrec
    def _insertInto(_s: Long, _slot: Slot): Unit = {
      val prev = T(_s)
      if (prev.isEmpty) {
        T(_s) = _slot
      } else {
        T(_s) = if (prev.isOccupied) _slot.setOccupied else _slot
        _insertInto(
          incrementIdx(_s),
          if (prev.isOccupied) prev.setShifted.clearOccupied
          else prev.setShifted
        )
      }
    }

    _insertInto(s, slot)
  }

  /* Remove the entry in QF[s] and slide the rest of the cluster forward. */
  private def deleteEntry(s: Long, quot: Long): Unit = {
    @tailrec
    def _deleteEntry(orig: Long, i: Long, q: Long): Unit = {
      val sp = incrementIdx(i)
      val next = T(sp)
      val curr_occupied = T(i).isOccupied
      if (next.isEmpty | next.isClusterStart | sp == orig) {
        T(i) = Slot.empty
      } else {
        val nextIsRunStart = next.isRunStart
        val _q = if (nextIsRunStart) findNextIdx(q, _.isOccupied) else q
        T(i) = {
          val nextShiftedProcessed =
            if (nextIsRunStart && curr_occupied && _q == i) {
              next.clearShifted
            } else next
          if (curr_occupied)
            nextShiftedProcessed.setOccupied
          else
            nextShiftedProcessed.clearOccupied
        }
        _deleteEntry(orig, sp, _q)
      }
    }

    _deleteEntry(s, s, quot)
  }

  def insert(element: E): Unit = {
    @tailrec
    def _idxToInsert(s: Long, rem: Long): Long = {
      val r = T(s).reminder
      if (r >= rem) {
        s
      } else {
        val sp = incrementIdx(s)
        if (!T(sp).isContinuation) sp else _idxToInsert(sp, rem)
      }
    }

    if (_nEntries >= capacity) throw CapacityOverflowedError(capacity)

    val (fq, fr) = quotientAndReminder(hash(serializer(element)))
    val T_fq = T(fq)

    if (T_fq.isEmpty) {
      T(fq) = Slot.empty.setOccupied.setReminder(fr)
      _nEntries += 1
    } else {
      if (!T_fq.isOccupied) {
        T(fq) = T_fq.setOccupied
      }

      /* Move the cursor to the insert position in the fq run. */
      val start = findRunIndex(fq)
      val p = if (T_fq.isOccupied) _idxToInsert(start, fr) else start

      /* The old start-of-run becomes a continuation. */
      if (T_fq.isOccupied) {
        if (p == start) {
          val old_head = T(start)
          T(start) = old_head.setContinuation
        }
      }

      insertInto(
        p, {
          /* The new element becomes a continuation. */
          val cont =
            if (T_fq.isOccupied && p != start) Slot.empty.setContinuation
            else Slot.empty
          /* Set the shifted bit if we can't use the canonical slot. */
          val shift = if (p != fq) cont.setShifted else cont
          shift.setReminder(fr)
        }
      )
      _nEntries += 1
    }
  }

  def maybeContains(element: E): Boolean = {
    val (fq, fr) = quotientAndReminder(hash(serializer(element)))
    val T_fq = T(fq)

    /* If this quotient has no run, give up. */
    if (!T_fq.isOccupied) {
      false
    } else {
      /* Scan the sorted run for the target remainder. */
      @tailrec
      def _scan(i: Long): Boolean = {
        val rem = T(i).reminder
        if (rem == fr) {
          true
        } else if (rem > fr) {
          false
        } else {
          val ip = incrementIdx(i)
          if (!T(ip).isContinuation) false else _scan(ip)
        }
      }

      val s = findRunIndex(fq)
      _scan(s)
    }
  }

  def remove(element: E): Unit = {
    val (fq, fr) = quotientAndReminder(hash(serializer(element)))
    val T_fq = T(fq)
    if (T_fq.isOccupied && (_nEntries != 0)) {

      /* Find the offending index */
      @tailrec
      def findOffendingIdx(s: Long, reminder: Long): (Long, Long) = {
        val r = T(s).reminder
        if (r >= reminder) {
          (r, s)
        } else {
          val sp = incrementIdx(s)
          if (!T(sp).isContinuation) (r, sp)
          else findOffendingIdx(sp, reminder)
        }
      }

      val start: Long = findRunIndex(fq)
      val (rem, s) = findOffendingIdx(start, fr)

      // if rem > fr, fr doesn't exist in this filter(already removed)
      if (rem == fr) {
        val kill = if (s == fq) {
          T_fq
        } else {
          T(s)
        }

        val replace_run_start = kill.isRunStart
        /* If we're deleting the last entry in a run, clear `is_occupied'. */
        if (replace_run_start) {
          val next = T(incrementIdx(s))
          if (!next.isContinuation) {
            T(fq) = T_fq.clearOccupied
          }
        }

        deleteEntry(s, fq)

        if (replace_run_start) {
          val next = T(s)
          val updated_next = {
            /* The new start-of-run is no longer a continuation. */
            val n1 = if (next.isContinuation) next.clearContinuation else next
            /* The new start-of-run is in the canonical slot. */
            val n2 = if ((s == fq) && n1.isRunStart) n1.clearShifted else n1
            n2
          }
          if (updated_next != next) T(s) = updated_next
        }
        _nEntries -= 1
      }
    }
  }
}

object QuotientFilter {
  def apply[E](cfg: Configs,
               hash: Array[Byte] => Long = FNV.hash64a(_).toLong)(
      implicit serializer: E => Array[Byte]) = {
    new QuotientFilter[E](cfg, CompactQFTable()(cfg), hash)
  }
}
