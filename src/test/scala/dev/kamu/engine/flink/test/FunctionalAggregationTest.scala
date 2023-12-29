package dev.kamu.engine.flink.test

import better.files.File
import dev.kamu.core.manifests._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.Engine
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.time.ZoneId

class FunctionalAggregationTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Tumbling window aggregation") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tEnv = StreamTableEnvironment.create(env)
      tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

      env.setParallelism(1)

      val engine = new Engine(env, tEnv)

      val tickersDataPath = tempDir.resolve("tickers")
      ParquetHelpers.write(
        tickersDataPath,
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

      val checkpointPath = tempDir.resolve("checkpoint")
      File(checkpointPath).createDirectories()
      val outputDataPath = tempDir.resolve("output")

      engine.executeRequest(
        ExecuteQueryRequest(
          datasetId = DatasetId("did:odf:abcdef"),
          datasetAlias = DatasetAlias("output"),
          systemTime = ts(10).toInstant,
          nextOffset = 0,
          vocab = DatasetVocabulary(),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            query =
              Some("""
                  |SELECT
                  | TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
                  | symbol as symbol,
                  | min(price) as `min`,
                  | max(price) as `max`
                  |FROM `tickers`
                  |GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
                  |""".stripMargin),
            queries = None,
            temporalTables = None
          ),
          queryInputs = Vector(
            ExecuteQueryRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("tickers"),
              queryAlias = "tickers",
              vocab = DatasetVocabulary().withDefaults(),
              offsetInterval = Some(OffsetInterval(start = 0, end = 11)),
              dataPaths = Vector(tickersDataPath),
              schemaFile = tickersDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(3, 2).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          newDataPath = outputDataPath
        )
      )

      val actual = ParquetHelpers
        .read[TickerSummary](outputDataPath)
        .sortBy(i => (i.event_time.getTime, i.symbol))

      actual shouldEqual List(
        TickerSummary(ts(10), ts(1), "A", 10, 11),
        TickerSummary(ts(10), ts(1), "B", 20, 21),
        TickerSummary(ts(10), ts(2), "A", 12, 13),
        TickerSummary(ts(10), ts(2), "B", 22, 23)
      )
    }
  }
}
