package dev.kamu.engine.flink.test

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer

class StreamSink extends SinkFunction[Row] {
  StreamSink.buffer = new ListBuffer[Row]()

  override def invoke(value: Row, context: SinkFunction.Context): Unit = {
    StreamSink.buffer.append(value)
  }

  def collect(): List[Row] = {
    val l = StreamSink.buffer.toList
    StreamSink.buffer = null
    l
  }
}

class StringStreamSkink extends StreamSink {
  def collectStr(): List[String] = {
    super.collect().map(_.toString)
  }
}

object StreamSink {
  private var buffer: ListBuffer[Row] = _

  def rowSink(): StreamSink = {
    new StreamSink()
  }

  def stringSink(): StringStreamSkink = {
    new StringStreamSkink()
  }
}
