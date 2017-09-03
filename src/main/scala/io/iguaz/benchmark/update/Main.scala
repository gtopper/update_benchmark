package io.iguaz.benchmark.update

import java.nio.file.Paths
import java.util.Properties

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import io.iguaz.v3io.api.container.ContainerID
import io.iguaz.v3io.kv.{KeyValueOperations, OverwriteMode, SimpleRow, UpdateEntry}

object Main {

  def main(args: Array[String]): Unit = {

    val collection = Paths.get(args(0))

    val printPeriod = sys.props.get("print-period").map(_.toInt).getOrElse(10000)

    val parallelMassUpdates = sys.props.get("parallel-mass-updates").map(_.toInt).getOrElse(1)
    val maxInFlightPropName = "v3io.config.kv.update.max-in-flight"
    val payloadSizePropName = "kv.update.payload.size"
    val maxInFlightOption = sys.props.get(maxInFlightPropName).map(_.toInt)
    val payloadSize = sys.props.get(payloadSizePropName).map(_.toLong)
    val capnpFactorPropName = "capn-message-size-ratio"
    val capnpFactor = sys.props.get(capnpFactorPropName).map(_.toInt)

    println(s"parallelMassUpdates = $parallelMassUpdates")
    println(s"collection = $collection")
    println(s"printPeriod = $printPeriod")
    println(s"maxInFlightOption = $maxInFlightOption")
    println(s"payloadSize = $payloadSize")
    println(s"capnpFactor = $capnpFactor")

    val start = System.currentTimeMillis()

    //    val maxInFlight = maxInFlightOption.get

    //    val semaphore = new Semaphore(maxInFlight)

    def requestIterator() = {
      var lastCycleStart = start
      Iterator.from(0).map { count =>
        if (count % printPeriod == 0) {
          val cycleStart = System.currentTimeMillis()
          val secondPassed = (cycleStart - start) / 1000
          val millisSinceLastCycle = cycleStart - lastCycleStart
          val millisRate = if (count == 0) 0 else printPeriod / millisSinceLastCycle
          val secondsRate = millisRate * 1000
          //          val numInFlight = maxInFlight - semaphore.availablePermits()
          //          println(s"[$secondsRate/sec | $numInFlight/$maxInFlight in-flight]\t$count entries written after $secondPassed seconds...")
          println(s"[$secondsRate/sec]\t$count entries written after $secondPassed seconds...")
          lastCycleStart = cycleStart
        }
        val row = SimpleRow(count.toString, Map("index" -> count.toLong))
        UpdateEntry(collection, row, OverwriteMode.REPLACE)
      }
    }

    val props = new Properties
    props.put("container-id", "1")

    val params = Map.empty ++
      maxInFlightOption.map(maxInFlightPropName -> _) ++
      payloadSize.map(payloadSizePropName -> _) ++
      capnpFactor.map(capnpFactorPropName -> _)

    val kvOps = KeyValueOperations(ContainerID(1), params)

    //    requestIterator().foreach { req =>
    //      semaphore.acquire()
    //      val f = kvOps.update(req.collection, req.row, req.mode)
    //      f.onComplete(_ => semaphore.release())
    //    }

    val responseFutureList = List.fill(parallelMassUpdates)(kvOps.updateMultiple(requestIterator()))
    val responsesFuture = Future.sequence(responseFutureList)
    Await.result(responsesFuture, Duration.Inf)
  }
}
