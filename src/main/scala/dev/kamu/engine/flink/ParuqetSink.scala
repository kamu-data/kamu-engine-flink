package dev.kamu.engine.flink

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.mutable.ArrayBuffer

class ParuqetSink(avroSchemaString: String, path: String)
    extends RichSinkFunction[GenericRecord] {
  @transient private lazy val rows: ArrayBuffer[GenericRecord] =
    ArrayBuffer.empty

  override def invoke(
    value: GenericRecord,
    context: SinkFunction.Context[_]
  ): Unit = {
    rows.append(value)
  }

  override def close(): Unit = {
    val avroSchema = new Schema.Parser().parse(avroSchemaString)

    val writer = AvroParquetWriter
      .builder[GenericRecord](new Path(path))
      .withSchema(avroSchema)
      .withDataModel(GenericData.get)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    for (row <- rows) {
      writer.write(row)
    }

    writer.close()
  }
}
