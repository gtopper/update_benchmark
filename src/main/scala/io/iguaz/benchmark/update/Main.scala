package io.iguaz.benchmark.update

import java.net.URI

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import io.iguaz.v3io.api.container.ContainerID
import io.iguaz.v3io.kv._

object Main {

  def main(args: Array[String]): Unit = {

    val collectionUri = new URI("v3io", "1", args(0), null, null)

    val parallelMassUpdates = sys.props.get("parallel-mass-updates").map(_.toInt).getOrElse(1)
    val maxInFlightPropName = "v3io.config.kv.update.max-in-flight"
    val payloadSizePropName = "kv.update.payload.size"
    val maxInFlightOption = sys.props.get(maxInFlightPropName).map(_.toInt)
    val payloadSize = sys.props.get(payloadSizePropName).map(_.toLong)
    val capnpFactorPropName = "capn-message-size-ratio"
    val capnpFactor = sys.props.get(capnpFactorPropName).map(_.toInt)

    println(s"parallelMassUpdates = $parallelMassUpdates")
    println(s"collectionUri = $collectionUri")
    println(s"maxInFlightOption = $maxInFlightOption")
    println(s"payloadSize = $payloadSize")
    println(s"capnpFactor = $capnpFactor")

    def requestIterator() = Iterator.from(0).zip(PrintPeriodIterator.create()).map(_._1).map { count =>
      val row = Row(count.toString, Map("index" -> count.toLong))
      UpdateEntry(collectionUri, row, OverwriteMode.REPLACE)
    }

    val params = Map.empty ++
      maxInFlightOption.map(maxInFlightPropName -> _) ++
      payloadSize.map(payloadSizePropName -> _) ++
      capnpFactor.map(capnpFactorPropName -> _)

    val kvOps = KeyValueOperations(ContainerID(1), params)

    val responseFutureList = List.fill(parallelMassUpdates)(kvOps.updateItems(requestIterator()))
    val responsesFuture = Future.sequence(responseFutureList)
    Await.result(responsesFuture, Duration.Inf)
  }
}
