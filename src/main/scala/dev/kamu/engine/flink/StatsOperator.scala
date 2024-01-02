package dev.kamu.engine.flink

import java.io.FileWriter
import org.apache.flink.runtime.state.StateSnapshotContext
import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.slf4j.LoggerFactory

class StatsOperator[T](
  name: String,
  path: String,
  flushOnClose: Boolean = false
) extends AbstractStreamOperator[T]
    with OneInputStreamOperator[T, T] {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private var rowCount = 0

  private var lastWatermark = Long.MinValue

  override def processElement(element: StreamRecord[T]): Unit = {
    rowCount += 1
    output.collect(element)
  }

  override def processWatermark(mark: Watermark): Unit = {
    lastWatermark = mark.getTimestamp
    super.processWatermark(mark)
  }

  private def flush(): Unit = {
    val writer = new FileWriter(path, false)
    writer.write(rowCount.toString + "\n")
    writer.write(lastWatermark.toString + "\n")
    writer.close()

    logger.info(s"Written stats for $name to: $path ($rowCount rows)")
    rowCount = 0
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    flush()
    super.snapshotState(context)
  }

  override def close(): Unit = {
    // For batch mode only
    if (flushOnClose) {
      flush()
    }
    if (rowCount > 0) {
      throw new RuntimeException(
        s"Closing stats for $name with $rowCount rows were not flushed"
      )
    }
  }
}
