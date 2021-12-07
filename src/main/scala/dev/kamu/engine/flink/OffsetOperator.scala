package dev.kamu.engine.flink

import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

class OffsetOperator(startOffset: Long)
    extends AbstractStreamOperator[Row]
    with OneInputStreamOperator[Row, Row] {

  private val logger = LoggerFactory.getLogger(getClass)

  private var offset = startOffset;

  override def processElement(element: StreamRecord[Row]): Unit = {
    element.getValue.setField(0, offset)
    offset += 1
    output.collect(element)
  }
}
