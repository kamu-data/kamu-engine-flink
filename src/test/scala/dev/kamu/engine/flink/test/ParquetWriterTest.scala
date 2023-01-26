package dev.kamu.engine.flink.test

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.{ParquetFilesStreamSourceFunction, ParuqetSink}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.formats.avro.RowDataToAvroConverters
import org.apache.flink.formats.avro.typeutils.{
  AvroSchemaConverter,
  GenericRecordAvroTypeInfo
}
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverterKamu
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormatKamu
import org.apache.flink.table.data.conversion.RowRowConverter
import org.apache.flink.table.runtime.typeutils.{
  ExternalTypeInfo,
  InternalTypeInfo
}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class ParquetWriterTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  ///////////////////////////////////////////////////////////////////////////////////////

  def runTest[IN: Encoder: Decoder, OUT: Encoder: Decoder](
    input: List[IN],
    query: String,
    expected: List[OUT]
  )(
    implicit schemaForIn: SchemaFor[IN],
    schemaForOut: SchemaFor[OUT]
  ) {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val inPath = tempDir.resolve(randomDataFileName())
      val outPath = tempDir.resolve(randomDataFileName())

      ParquetHelpers.write(inPath, input)

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setRestartStrategy(RestartStrategies.noRestart())
      env.getConfig.disableGenericTypes()
      env.setParallelism(1)

      val tEnv = StreamTableEnvironment.create(env)

      val parquetSchema = ParquetHelpers.getSchemaFromFile(inPath)
      println(s"Parquet schema: ${parquetSchema}")

      val rowType = ParquetSchemaConverterKamu.convertToRowType(parquetSchema)
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

      val sourceFunction = new ParquetFilesStreamSourceFunction(
        "src",
        Vector(inPath.toString),
        parquetFormat,
        row => row.getTimestamp(2, Int.MaxValue).getMillisecond,
        None,
        Vector.empty,
        true,
        None
      )

      val stream = env.addSource(sourceFunction)(typeInfo)
      println(s"Data stream type: ${stream.dataType}")

      val table = tEnv
        .fromDataStream(
          stream
        )
      tEnv.createTemporaryView("input", table)
      println(s"Table schema: ${table.getResolvedSchema}")

      val resultStream = tEnv
        .sqlQuery(query)
        .toDataStream

      val resultStreamDataType =
        resultStream.dataType.asInstanceOf[ExternalTypeInfo[Row]].getDataType

      val converterRowData =
        RowRowConverter.create(resultStreamDataType)

      val resultStreamRowData =
        resultStream.map(row => converterRowData.toInternal(row))(
          InternalTypeInfo.of(resultStreamDataType.getLogicalType)
        )

      val avroSchema =
        AvroSchemaConverter.convertToSchema(resultStreamDataType.getLogicalType)
      println(s"Result schema in Avro format:\n${avroSchema.toString(true)}")

      val converterAvro = RowDataToAvroConverters.createConverter(
        resultStreamDataType.getLogicalType
      )

      val resultStreamAvro = resultStreamRowData.map(
        row =>
          converterAvro.convert(avroSchema, row).asInstanceOf[GenericRecord]
      )(new GenericRecordAvroTypeInfo(avroSchema))

      resultStreamAvro.print()

      resultStreamAvro.addSink(
        new ParuqetSink(
          avroSchema.toString(),
          outPath.toString,
          true
        )
      )

      env.execute()

      val actual = ParquetHelpers.read[OUT](outPath)
      actual shouldEqual expected
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("basic") {
    runTest(
      List(
        Ticker(0, ts(5), ts(1, 1), "A", 10),
        Ticker(1, ts(5), ts(1, 1), "B", 20),
        Ticker(2, ts(5), ts(1, 2), "A", 11),
        Ticker(3, ts(5), ts(1, 2), "B", 21)
      ),
      """SELECT
        |  `offset`,
        |  `system_time`,
        |  `event_time`,
        |  `symbol`,
        |  `price` * 10 as `price`
        |FROM input""".stripMargin,
      List(
        Ticker(0, ts(5), ts(1, 1), "A", 100),
        Ticker(1, ts(5), ts(1, 1), "B", 200),
        Ticker(2, ts(5), ts(1, 2), "A", 110),
        Ticker(3, ts(5), ts(1, 2), "B", 210)
      )
    )
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("decimal") {
    runTest(
      List(
        WriteRaw(0, ts(1), ts(1, 1), "123456789.0123"),
        WriteRaw(1, ts(1), ts(1, 2), "-123456789.0123"),
        WriteRaw(
          2,
          ts(1),
          ts(1, 3),
          "12345678901234567890.123456789012345678"
        ),
        WriteRaw(
          3,
          ts(1),
          ts(1, 4),
          "-12345678901234567890.123456789012345678"
        )
      ),
      """SELECT
        |  `event_time` as `system_time`,
        |  `event_time`,
        |  TRY_CAST (`value` as DECIMAL(13, 4)) as decimal_13_4,
        |  TRY_CAST (`value` as DECIMAL(38, 18)) as decimal_38_18
        |FROM input""".stripMargin,
      List(
        WriteResult(
          ts(1, 1),
          ts(1, 1),
          Some(BigDecimal("123456789.0123")),
          Some(BigDecimal("123456789.0123"))
        ),
        WriteResult(
          ts(1, 2),
          ts(1, 2),
          Some(BigDecimal("-123456789.0123")),
          Some(BigDecimal("-123456789.0123"))
        ),
        WriteResult(
          ts(1, 3),
          ts(1, 3),
          None,
          Some(BigDecimal("12345678901234567890.123456789012345678"))
        ),
        WriteResult(
          ts(1, 4),
          ts(1, 4),
          None,
          Some(BigDecimal("-12345678901234567890.123456789012345678"))
        )
      )
    )
  }

  ///////////////////////////////////////////////////////////////////////////////////////

}
