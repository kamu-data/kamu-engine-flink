package dev.kamu.engine.flink

import org.apache.flink.streaming.api.operators.{
  AbstractStreamOperator,
  OneInputStreamOperator
}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.types.{Row, RowKind}

class ChangelogOperator(opFieldIndex: Int)
    extends AbstractStreamOperator[Row]
    with OneInputStreamOperator[Row, Row] {

  override def processElement(element: StreamRecord[Row]): Unit = {
    val op: Int = element.getValue.getKind match {
      case RowKind.INSERT        => Op.Append
      case RowKind.DELETE        => Op.Retract
      case RowKind.UPDATE_BEFORE => Op.CorrectFrom
      case RowKind.UPDATE_AFTER  => Op.CorrectTo
    }
    element.getValue.setField(opFieldIndex, op)
    output.collect(element)
  }
}
