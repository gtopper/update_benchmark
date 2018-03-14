package io.iguaz.benchmark.update

import java.text.NumberFormat
import java.util.Locale

import com.typesafe.scalalogging.LazyLogging

object PrintPeriodIterator extends LazyLogging {

  private val numberFormat = NumberFormat.getIntegerInstance(Locale.US)

  private val printPeriod = sys.props.getOrElse("print-period", "5000").toInt

  logger.info(s"printPeriod=$printPeriod")

  def create(): Iterator[Unit] = {
    val start = System.currentTimeMillis()
    var lastCycleStart = start
    var lastCount = 0L
    Iterator.from(1).map { count =>
      val cycleStart = System.currentTimeMillis()
      if (cycleStart >= lastCycleStart + printPeriod) {
        val millisSinceLastCycle = cycleStart - lastCycleStart
        val progress = count - lastCount
        val secondPassed = (cycleStart - start) / 1000L
        val secondPassedStr = numberFormat.format(secondPassed)
        val secondsRate = progress * 1000L / millisSinceLastCycle
        val secondsRateStr = numberFormat.format(secondsRate)
        val countStr = numberFormat.format(count)
        logger.info(s"[$secondsRateStr/s] $countStr entries written after $secondPassedStr seconds...")
        lastCycleStart = cycleStart
        lastCount = count
      }
    }
  }
}
