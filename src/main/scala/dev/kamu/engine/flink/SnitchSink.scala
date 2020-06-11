package dev.kamu.engine.flink

import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.flink.types.Row

class SnitchSink(name: String)
    extends RichSinkFunction[Row]
    with CheckpointedFunction {

  override def invoke(
    value: Row,
    context: SinkFunction.Context[_]
  ): Unit = {
    println(
      s"###### $name > [${Thread.currentThread().getId}] $value"
    )
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println(s"###### $name > SNAPSHOT")
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def close(): Unit = {
    println(s"###### $name > [${Thread.currentThread().getId}] CLOSE")
  }
}
