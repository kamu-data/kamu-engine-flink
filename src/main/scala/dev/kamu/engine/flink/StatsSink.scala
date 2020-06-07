package dev.kamu.engine.flink

import java.io.FileWriter
import java.security.MessageDigest

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
import org.apache.log4j.LogManager

class StatsSink(path: String)
    extends RichSinkFunction[Row]
    with CheckpointedFunction {
  @transient private lazy val logger = LogManager.getLogger(getClass.getName)

  @transient private lazy val digest = MessageDigest.getInstance("sha-256")

  private var rowCount = 0

  override def invoke(
    value: Row,
    context: SinkFunction.Context[_]
  ): Unit = {
    rowCount += 1
    digest.update(value.toString.getBytes("utf-8"))
  }

  private def flush(): Unit = {
    val hash = digest.digest().map("%02x".format(_)).mkString

    val writer = new FileWriter(path, false)
    writer.write(rowCount.toString + "\n")
    writer.write(hash + "\n")
    writer.close()

    println(s"Written stats to: $path")

    rowCount = 0
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    flush()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def close(): Unit = {
    if (rowCount > 0) {
      throw new RuntimeException(
        s"Closing with $rowCount rows still in the buffer"
      )
    }
  }
}
