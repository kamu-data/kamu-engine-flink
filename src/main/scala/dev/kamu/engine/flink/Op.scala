package dev.kamu.engine.flink

import org.apache.flink.types.RowKind

object Op {
  val Append: Int = 0
  val Retract: Int = 1
  val CorrectFrom: Int = 2
  val CorrectTo: Int = 3

  def toRowKind(value: Int): RowKind = {
    value match {
      case Append      => RowKind.INSERT
      case Retract     => RowKind.DELETE
      case CorrectFrom => RowKind.UPDATE_BEFORE
      case CorrectTo   => RowKind.UPDATE_AFTER
    }
  }
}
