package io.iguaz.benchmark.update

import java.net.URI

import io.iguaz.v3io.kv._

object Main {

  def main(args: Array[String]): Unit = {

    val collectionUri = new URI("v3io", "1", args(0), null, null)

    val parallelMassUpdates = sys.props.get("parallel-mass-updates").map(_.toInt).getOrElse(1)
    val payloadSizePropName = "kv.update.payload.size"
    val payloadSize = sys.props.get(payloadSizePropName).map(_.toLong)
    val capnpFactorPropName = "capn-message-size-ratio"
    val capnpFactor = sys.props.get(capnpFactorPropName).map(_.toInt)

    println(s"parallelMassUpdates = $parallelMassUpdates")
    println(s"collectionUri = $collectionUri")
    println(s"payloadSize = $payloadSize")
    println(s"capnpFactor = $capnpFactor")

    def requestIterator() = Iterator.from(0).map { count =>
      val row = Row(count.toString, Map("index" -> count.toLong))
      UpdateEntry(collectionUri, row, OverwriteMode.REPLACE)
    }

    val params = Map.empty ++
      payloadSize.map(payloadSizePropName -> _) ++
      capnpFactor.map(capnpFactorPropName -> _)

    val kvOps = KeyValueOperations(params)

    val responseIteratorList = List.fill(parallelMassUpdates)(kvOps.updateItemsIterator(requestIterator()).map(_ => ()))
    val responsesIterator = responseIteratorList.foldRight(Iterator.continually(())) {
      case (acc, x) => acc.zip(x).map(_ => ())
    }
    responsesIterator.zip(PrintPeriodIterator.create()).foreach(_ => ())
  }
}
