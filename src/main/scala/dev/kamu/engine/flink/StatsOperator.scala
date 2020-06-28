package dev.kamu.engine.flink

import java.io.FileWriter
import java.security.MessageDigest

import org.apache.flink.runtime.state.StateSnapshotContext
import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager

class StatsOperator(datasetID: String, path: String)
    extends AbstractStreamOperator[Row]
    with OneInputStreamOperator[Row, Row] {
  @transient private lazy val logger = LogManager.getLogger(getClass.getName)

  @transient private lazy val digest = MessageDigest.getInstance("sha-256")

  private var rowCount = 0

  private var lastWatermark = Long.MinValue

  override def processElement(element: StreamRecord[Row]): Unit = {
    rowCount += 1
    output.collect(element)
    digest.update(element.getValue.toString.getBytes("utf-8"))
  }

  override def processWatermark(mark: Watermark): Unit = {
    lastWatermark = mark.getTimestamp
    super.processWatermark(mark)
  }

  private def flush(): Unit = {
    val hash = digest.digest().map("%02x".format(_)).mkString

    val writer = new FileWriter(path, false)
    writer.write(rowCount.toString + "\n")
    writer.write(lastWatermark.toString + "\n")
    writer.write(hash + "\n")
    writer.close()

    logger.info(s"Written stats to: $path ($rowCount rows)")
    rowCount = 0
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    flush()
    super.snapshotState(context)
  }

  override def close(): Unit = {
    if (rowCount > 0) {
      throw new RuntimeException(
        s"Closing with $rowCount rows were not flushed"
      )
    }
  }
}
