package dev.kamu.engine.flink

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class ParuqetSink(
  avroSchemaString: String,
  path: String,
  flushOnClose: Boolean = false
) extends RichSinkFunction[GenericRecord]
    with CheckpointedFunction {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private lazy val rows: ArrayBuffer[GenericRecord] =
    ArrayBuffer.empty

  @transient private var flushed: Boolean = false

  override def invoke(
    value: GenericRecord,
    context: SinkFunction.Context
  ): Unit = {
    if (flushed)
      throw new RuntimeException(
        "Attempting to append row after sink was already flushed"
      )
    rows.append(value)
  }

  private def flush(): Unit = {
    if (flushed)
      throw new RuntimeException("Attempting to flush sink twice")

    flushed = true

    if (rows.isEmpty) {
      logger.info("No data on flush")
      return
    }

    logger.info(s"Flushing the parquet sink (${rows.size} rows)")

    val avroSchema = new Schema.Parser().parse(avroSchemaString)

    val model = new GenericData()
    model.addLogicalTypeConversion(
      new AvroConversions.LocalDateTimeConversion()
    )

    val writer = AvroParquetWriter
      .builder[GenericRecord](new Path(path))
      .withSchema(avroSchema)
      .withDataModel(model)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    for (row <- rows) {
      writer.write(row)
    }

    writer.close()
    rows.clear()

    logger.info(s"Written parquet file to: $path")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    flush()
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {}

  override def close(): Unit = {
    // For testing purposes only
    if (flushOnClose)
      flush()

    if (rows.nonEmpty) {
      throw new RuntimeException(
        s"Closing with ${rows.size} rows still in the buffer"
      )
    }
  }
}
