package dev.kamu.engine.flink

import java.time.Instant
import org.apache.flink.runtime.state.StateSnapshotContext
import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.slf4j.LoggerFactory

class SnitchOperator[T](name: String)
    extends AbstractStreamOperator[T]
    with OneInputStreamOperator[T, T] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def processElement(element: StreamRecord[T]): Unit = {
    logger.info(
      s"###### $name > [${Thread.currentThread().getId}] ${element.getValue}"
    )
    output.collect(element)
  }

  override def processWatermark(mark: Watermark): Unit = {
    val instant = Instant.ofEpochMilli(mark.getTimestamp)
    logger.info(
      s"###### $name > [${Thread.currentThread().getId}] WM $instant"
    )
    super.processWatermark(mark)
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    logger.info(s"###### $name > SNAPSHOT")
  }

  override def close(): Unit = {
    logger.info(s"###### $name > [${Thread.currentThread().getId}] CLOSE")
  }
}
