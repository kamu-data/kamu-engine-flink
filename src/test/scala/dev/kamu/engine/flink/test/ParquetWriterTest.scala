package dev.kamu.engine.flink.test

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.{Op, ParquetFilesStreamSourceFunction, ParuqetSink}
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
import org.apache.flink.types.{Row, RowKind}
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
    expectedSchema: String,
    expectedData: List[OUT]
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
        _ => RowKind.INSERT,
        _ => 0,
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

      val schema = ParquetHelpers.getSchemaFromFile(outPath)
      schema.toString shouldEqual expectedSchema

      val actual = ParquetHelpers.read[OUT](outPath)
      actual shouldEqual expectedData
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("basic") {
    runTest(
      List(
        Ticker(0, Op.Append, ts(5), ts(1, 1), "A", 10),
        Ticker(1, Op.Append, ts(5), ts(1, 1), "B", 20),
        Ticker(2, Op.Append, ts(5), ts(1, 2), "A", 11),
        Ticker(3, Op.Append, ts(5), ts(1, 2), "B", 21)
      ),
      """SELECT
        |  `offset`,
        |  `op`,
        |  `system_time`,
        |  `event_time`,
        |  `symbol`,
        |  `price` * 10 as `price`
        |FROM input""".stripMargin,
      """message org.apache.flink.avro.generated.record {
        |  required int64 offset;
        |  required int32 op;
        |  required int64 system_time (TIMESTAMP(MILLIS,true));
        |  required int64 event_time (TIMESTAMP(MILLIS,true));
        |  required binary symbol (STRING);
        |  required int32 price;
        |}
        |""".stripMargin,
      List(
        Ticker(0, Op.Append, ts(5), ts(1, 1), "A", 100),
        Ticker(1, Op.Append, ts(5), ts(1, 1), "B", 200),
        Ticker(2, Op.Append, ts(5), ts(1, 2), "A", 110),
        Ticker(3, Op.Append, ts(5), ts(1, 2), "B", 210)
      )
    )
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  test("decimal") {
    runTest(
      List(
        ValueRaw("123456789.0123"),
        ValueRaw("-123456789.0123"),
        ValueRaw("12345678901234567890.123456789012345678"),
        ValueRaw("-12345678901234567890.123456789012345678")
      ),
      """SELECT
        |  TRY_CAST (`value` as DECIMAL(13, 4)) as decimal_13_4,
        |  TRY_CAST (`value` as DECIMAL(38, 18)) as decimal_38_18
        |FROM input""".stripMargin,
      """message org.apache.flink.avro.generated.record {
        |  optional binary decimal_13_4 (DECIMAL(13,4));
        |  optional binary decimal_38_18 (DECIMAL(38,18));
        |}
        |""".stripMargin,
      List(
        ValueDecimalPrecision(
          Some(BigDecimal("123456789.0123")),
          Some(BigDecimal("123456789.0123"))
        ),
        ValueDecimalPrecision(
          Some(BigDecimal("-123456789.0123")),
          Some(BigDecimal("-123456789.0123"))
        ),
        ValueDecimalPrecision(
          None,
          Some(BigDecimal("12345678901234567890.123456789012345678"))
        ),
        ValueDecimalPrecision(
          None,
          Some(BigDecimal("-12345678901234567890.123456789012345678"))
        )
      )
    )
  }

  ///////////////////////////////////////////////////////////////////////////////////////

}
