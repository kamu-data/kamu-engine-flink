package dev.kamu.engine.flink.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.{
  MaxOutOfOrderWatermarkStrategy,
  ParquetFilesStreamSourceFunction
}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverterKamu
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormatKamu
import org.apache.flink.table.api.{FieldExpression, Schema, WithOperations}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical._
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.time.Instant

class ParquetSourceTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  ///////////////////////////////////////////////////////////////////////////////////////

  test("Read Multiple Files") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val dataPaths = Array(
        tempDir.resolve(randomDataFileName()),
        tempDir.resolve(randomDataFileName()),
        tempDir.resolve(randomDataFileName())
      )
      val sourceExhaustedMarkerPath = tempDir.resolve("marker")

      ParquetHelpers.write(
        dataPaths(0),
        Seq(
          Ticker(0, ts(5), ts(1, 1), "A", 10),
          Ticker(1, ts(5), ts(1, 1), "B", 20),
          Ticker(2, ts(5), ts(1, 2), "A", 11),
          Ticker(3, ts(5), ts(1, 2), "B", 21)
        )
      )
      ParquetHelpers.write(
        dataPaths(1),
        Seq(
          Ticker(4, ts(5), ts(2, 1), "A", 12),
          Ticker(5, ts(5), ts(2, 1), "B", 22),
          Ticker(6, ts(5), ts(2, 2), "A", 13),
          Ticker(7, ts(5), ts(2, 2), "B", 23)
        )
      )
      ParquetHelpers.write(
        dataPaths(2),
        Seq(
          Ticker(8, ts(5), ts(3, 1), "A", 14),
          Ticker(9, ts(5), ts(3, 1), "B", 24),
          Ticker(10, ts(5), ts(3, 2), "A", 15),
          Ticker(11, ts(5), ts(3, 2), "B", 25)
        )
      )

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setRestartStrategy(RestartStrategies.noRestart())
      env.getConfig.disableGenericTypes()
      env.setParallelism(1)

      val tEnv = StreamTableEnvironment.create(env)

      val parquetSchema = ParquetHelpers.getSchemaFromFile(dataPaths(0))
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
        dataPaths.map(_.toString).toVector,
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
          stream,
          Schema
            .newBuilder()
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build()
        )

      tEnv.createTemporaryView("test", table)
      println(s"Table schema: ${table.getResolvedSchema}")

      val sink = StreamSink.stringSink()

      tEnv
        .sqlQuery("SELECT * FROM test")
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

    ///////////////////////////////////////////////////////////////////////////////////////

  }
}
