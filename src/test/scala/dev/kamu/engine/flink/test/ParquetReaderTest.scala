package dev.kamu.engine.flink.test

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.fs._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import dev.kamu.core.utils.Temp
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.src.{FileSource, FileSourceSplit}
import org.apache.flink.formats.parquet.{
  ParquetColumnarRowInputFormat,
  ParquetColumnarRowInputFormatKamu
}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical._
import org.apache.hadoop.conf.Configuration
import org.apache.flink.core.fs.{Path => FlinkPath}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.formats.avro.AvroToRowDataConverters
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.avro.AvroParquetReaders
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverterKamu
import org.apache.parquet.avro.AvroSchemaConverter

class ParquetReaderTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  ///////////////////////////////////////////////////////////////////////////////////////

  test("Read Parquet - ParquetColumnarRowInputFormat") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val dataPath = tempDir.resolve(randomDataFileName())

      ParquetHelpers.write(
        dataPath,
        Seq(
          Ticker(0, ts(5), ts(1, 1), "A", 10),
          Ticker(1, ts(5), ts(1, 1), "B", 20),
          Ticker(2, ts(5), ts(1, 2), "A", 11),
          Ticker(3, ts(5), ts(1, 2), "B", 21),
          Ticker(4, ts(5), ts(2, 1), "A", 12),
          Ticker(5, ts(5), ts(2, 1), "B", 22),
          Ticker(6, ts(5), ts(2, 2), "A", 13),
          Ticker(7, ts(5), ts(2, 2), "B", 23),
          Ticker(8, ts(5), ts(3, 1), "A", 14),
          Ticker(9, ts(5), ts(3, 1), "B", 24),
          Ticker(10, ts(5), ts(3, 2), "A", 15),
          Ticker(11, ts(5), ts(3, 2), "B", 25)
        )
      )

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.getConfig.disableGenericTypes()
      env.setParallelism(1)

      val tEnv = StreamTableEnvironment.create(env)

      val parquetSchema = ParquetHelpers.getSchemaFromFile(dataPath)
      println(s"Parquet schema: ${parquetSchema}")

      //val rowType = ParquetSchemaConverter.convertToRowType(parquetSchema)
      val rowType = RowType.of(
        Array[LogicalType](
          new BigIntType(),
          new VarCharType(VarCharType.MAX_LENGTH)
        ),
        Array[String]("offset", "symbol")
      )
      println(s"RowType: ${rowType}")

      val typeInfo = InternalTypeInfo.of(rowType)

      val parquetFormat = new ParquetColumnarRowInputFormat[FileSourceSplit](
        new Configuration(),
        rowType,
        typeInfo,
        500,
        true,
        true
      );

      val fileSource = FileSource
        .forBulkFileFormat(
          parquetFormat,
          new FlinkPath(dataPath.toString)
        )
        .build()

      val stream =
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "blah")(
          typeInfo
        )

      println(s"Data stream type: ${stream.dataType}")

      tEnv.createTemporaryView("test", stream)

      val streamDeriv = tEnv
        .sqlQuery(
          "SELECT `offset`  FROM test"
        )
        .toDataStream

      streamDeriv.print()

      env.execute()
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("Read Parquet - AvroGenericRecord") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val dataPath = tempDir.resolve(randomDataFileName())

      ParquetHelpers.write(
        dataPath,
        Seq(
          Ticker(0, ts(5), ts(1, 1), "A", 10),
          Ticker(1, ts(5), ts(1, 1), "B", 20),
          Ticker(2, ts(5), ts(1, 2), "A", 11),
          Ticker(3, ts(5), ts(1, 2), "B", 21),
          Ticker(4, ts(5), ts(2, 1), "A", 12),
          Ticker(5, ts(5), ts(2, 1), "B", 22),
          Ticker(6, ts(5), ts(2, 2), "A", 13),
          Ticker(7, ts(5), ts(2, 2), "B", 23),
          Ticker(8, ts(5), ts(3, 1), "A", 14),
          Ticker(9, ts(5), ts(3, 1), "B", 24),
          Ticker(10, ts(5), ts(3, 2), "A", 15),
          Ticker(11, ts(5), ts(3, 2), "B", 25)
        )
      )

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.getConfig.disableGenericTypes()
      env.setParallelism(1)

      val tEnv = StreamTableEnvironment.create(env)

      val parquetSchema = ParquetHelpers.getSchemaFromFile(dataPath)
      println(s"Parquet schema: ${parquetSchema}")

      val schemaConverter = new AvroSchemaConverter(new Configuration())
      val avroSchema = schemaConverter.convert(parquetSchema)
      println(s"Avro schema: ${avroSchema}")

      val fileSource = FileSource
        .forRecordStreamFormat(
          AvroParquetReaders.forGenericRecord(avroSchema),
          new FlinkPath(dataPath.toString)
        )
        .build()

      val stream =
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "blah")(
          new GenericRecordAvroTypeInfo(avroSchema)
        )

      println(s"Data stream type: ${stream.dataType}")

      val rowType = RowType.of(
        Array[LogicalType](
          new BigIntType(),
          new VarCharType(VarCharType.MAX_LENGTH)
        ),
        Array[String]("offset", "symbol")
      )
      println(s"RowType: ${rowType}")

      val converter = AvroToRowDataConverters.createRowConverter(rowType)

      val convertedStream = stream.map(genericRecord => {
        val res = converter.convert(genericRecord);
        res
      })(InternalTypeInfo.of(rowType))

      println(s"Converted data stream type: ${convertedStream.dataType}")

      val table = tEnv.fromDataStream(convertedStream)
      table.printSchema()
      tEnv.createTemporaryView("test", table)

      val streamDeriv = tEnv
        .sqlQuery(
          "SELECT *  FROM test"
        )
        .toDataStream

      streamDeriv.print()

      env.execute()
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("Read Parquet - CustomInputFormat") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val dataPath = tempDir.resolve(randomDataFileName())

      ParquetHelpers.write(
        dataPath,
        Seq(
          Ticker(0, ts(5), ts(1, 1), "A", 10),
          Ticker(1, ts(5), ts(1, 1), "B", 20),
          Ticker(2, ts(5), ts(1, 2), "A", 11),
          Ticker(3, ts(5), ts(1, 2), "B", 21),
          Ticker(4, ts(5), ts(2, 1), "A", 12),
          Ticker(5, ts(5), ts(2, 1), "B", 22),
          Ticker(6, ts(5), ts(2, 2), "A", 13),
          Ticker(7, ts(5), ts(2, 2), "B", 23),
          Ticker(8, ts(5), ts(3, 1), "A", 14),
          Ticker(9, ts(5), ts(3, 1), "B", 24),
          Ticker(10, ts(5), ts(3, 2), "A", 15),
          Ticker(11, ts(5), ts(3, 2), "B", 25)
        )
      )

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.getConfig.disableGenericTypes()
      env.setParallelism(1)

      val tEnv = StreamTableEnvironment.create(env)

      val parquetSchema = ParquetHelpers.getSchemaFromFile(dataPath)
      println(s"Parquet schema: ${parquetSchema}")

      val rowType = ParquetSchemaConverterKamu.convertToRowType(parquetSchema)
      //      val rowType = RowType.of(
      //        Array[LogicalType](
      //          new BigIntType(),
      //          new TimestampType(),
      //          new TimestampType(),
      //          new VarCharType(VarCharType.MAX_LENGTH),
      //          new IntType()
      //        ),
      //        Array[String]("offset", "system_time", "event_time", "symbol", "price")
      //      )
      println(s"RowType: ${rowType}")

      val typeInfo = InternalTypeInfo.of(rowType)

      val parquetFormat =
        new ParquetColumnarRowInputFormatKamu[FileSourceSplit](
          new Configuration(),
          rowType,
          typeInfo,
          500,
          true,
          true
        );

      val fileSource = FileSource
        .forBulkFileFormat(
          parquetFormat,
          new FlinkPath(dataPath.toString)
        )
        .build()

      val stream =
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "blah")(
          typeInfo
        )
      println(s"Data stream type: ${stream.dataType}")

      tEnv.createTemporaryView("test", stream)

      val sink = StreamSink.stringSink()

      tEnv
        .sqlQuery(
          "SELECT * FROM test"
        )
        .toDataStream
        .addSink(sink)

      env.execute()

      val actual = sink.collectStr()
      val expected = List(
        "+I[0, 2000-01-05T00:00, 2000-01-01T01:00, A, 10]",
        "+I[1, 2000-01-05T00:00, 2000-01-01T01:00, B, 20]",
        "+I[2, 2000-01-05T00:00, 2000-01-01T02:00, A, 11]",
        "+I[3, 2000-01-05T00:00, 2000-01-01T02:00, B, 21]",
        "+I[4, 2000-01-05T00:00, 2000-01-02T01:00, A, 12]",
        "+I[5, 2000-01-05T00:00, 2000-01-02T01:00, B, 22]",
        "+I[6, 2000-01-05T00:00, 2000-01-02T02:00, A, 13]",
        "+I[7, 2000-01-05T00:00, 2000-01-02T02:00, B, 23]",
        "+I[8, 2000-01-05T00:00, 2000-01-03T01:00, A, 14]",
        "+I[9, 2000-01-05T00:00, 2000-01-03T01:00, B, 24]",
        "+I[10, 2000-01-05T00:00, 2000-01-03T02:00, A, 15]",
        "+I[11, 2000-01-05T00:00, 2000-01-03T02:00, B, 25]"
      )

      expected shouldEqual actual
    }
  }
}
