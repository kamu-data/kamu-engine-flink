package dev.kamu.engine.flink

import java.time.Instant

import org.apache.flink.runtime.state.StateSnapshotContext
import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.types.Row

class SnitchOperator(name: String)
    extends AbstractStreamOperator[Row]
    with OneInputStreamOperator[Row, Row] {

  override def processElement(element: StreamRecord[Row]): Unit = {
    println(
      s"###### $name > [${Thread.currentThread().getId}] ${element.getValue}"
    )
    output.collect(element)
  }

  override def processWatermark(mark: Watermark): Unit = {
    val instant = Instant.ofEpochMilli(mark.getTimestamp)
    println(
      s"###### $name > [${Thread.currentThread().getId}] WM $instant"
    )
    super.processWatermark(mark)
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    println(s"###### ${getOperatorID()} $name > SNAPSHOT")
  }

  override def close(): Unit = {
    println(s"###### $name > [${Thread.currentThread().getId}] CLOSE")
  }
}
