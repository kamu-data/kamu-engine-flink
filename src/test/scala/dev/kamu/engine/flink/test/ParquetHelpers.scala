package dev.kamu.engine.flink.test

import java.nio.file.Path

import com.sksamuel.avro4s.{
  AvroSchema,
  Decoder,
  Encoder,
  RecordFormat,
  SchemaFor
}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object ParquetHelpers {
  def write[T: Encoder: Decoder](path: Path, data: Seq[T])(
    implicit schemaFor: SchemaFor[T]
  ): Unit = {
    val avroSchema = AvroSchema[T]
    val format = RecordFormat[T]

    val records = data.map(format.to)

    println(avroSchema.toString(true))

    val writer = AvroParquetWriter
      .builder[GenericRecord](new org.apache.hadoop.fs.Path(path.toUri))
      .withSchema(avroSchema)
      .withDataModel(GenericData.get)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    records.foreach(writer.write)

    writer.close()
  }

  def read[T: Encoder: Decoder](path: Path)(
    implicit schemaFor: SchemaFor[T]
  ): List[T] = {
    val format = RecordFormat[T]

    val reader =
      AvroParquetReader
        .builder[GenericRecord](new org.apache.hadoop.fs.Path(path.toUri))
        .withDataModel(GenericData.get)
        .build()

    val records = Stream
      .continually(reader.read)
      .takeWhile(_ != null)
      .map(format.from)
      .toList

    reader.close()
    records
  }
}
