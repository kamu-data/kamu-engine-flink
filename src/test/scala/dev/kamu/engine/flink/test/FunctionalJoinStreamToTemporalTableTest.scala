package dev.kamu.engine.flink.test

import better.files.File
import dev.kamu.core.manifests.{
  DatasetID,
  DatasetName,
  DatasetVocabulary,
  ExecuteQueryInput,
  ExecuteQueryRequest,
  ExecuteQueryResponse,
  OffsetInterval,
  TemporalTable,
  Transform,
  Watermark
}
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.Engine
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.time.ZoneId

class FunctionalJoinStreamToTemporalTableTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Temporal table join (simple)") {
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
          Ticker(0, ts(5), ts(1), "A", 10),
          Ticker(1, ts(5), ts(1), "B", 20),
          Ticker(2, ts(5), ts(2), "A", 10),
          Ticker(3, ts(5), ts(2), "B", 20),
          Ticker(4, ts(5), ts(3), "A", 12),
          Ticker(5, ts(5), ts(3), "B", 22),
          Ticker(6, ts(5), ts(4), "A", 14),
          Ticker(7, ts(5), ts(4), "B", 24)
        )
      )

      val stocksDataPath = tempDir.resolve("stocks.owned")
      ParquetHelpers.write(
        stocksDataPath,
        Seq(
          StocksOwned(0, ts(4), ts(2), "A", 100),
          StocksOwned(1, ts(4), ts(3), "B", 200)
        )
      )

      val checkpointPath = tempDir.resolve("checkpoint")
      File(checkpointPath).createDirectories()
      val outputDataPath = tempDir.resolve("output")

      val response = engine.executeRequest(
        ExecuteQueryRequest(
          datasetID = DatasetID(""),
          datasetName = DatasetName("output"),
          systemTime = ts(10).toInstant,
          offset = 0,
          vocab = DatasetVocabulary(),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            query =
              Some("""
                  |SELECT
                  |  t.event_time,
                  |  t.symbol,
                  |  owned.volume as volume,
                  |  t.price as current_price,
                  |  owned.volume * t.price as current_value
                  |FROM
                  |  tickers as t,
                  |  LATERAL TABLE (`stocks.owned`(t.event_time)) AS owned
                  |WHERE t.symbol = owned.symbol
                  |""".stripMargin),
            queries = None,
            temporalTables = Some(
              Vector(
                TemporalTable(
                  name = "stocks.owned",
                  primaryKey = Vector("symbol")
                )
              )
            )
          ),
          inputs = Vector(
            ExecuteQueryInput(
              datasetID = DatasetID(""),
              datasetName = DatasetName("tickers"),
              vocab = DatasetVocabulary().withDefaults(),
              dataInterval = Some(OffsetInterval(start = 0, end = 7)),
              dataPaths = Vector(tickersDataPath),
              schemaFile = tickersDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(4).toInstant))
            ),
            ExecuteQueryInput(
              datasetID = DatasetID(""),
              datasetName = DatasetName("stocks.owned"),
              vocab = DatasetVocabulary().withDefaults(),
              dataInterval = Some(OffsetInterval(start = 0, end = 1)),
              dataPaths = Vector(stocksDataPath),
              schemaFile = stocksDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(3).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          outDataPath = outputDataPath
        )
      )

      response
        .asInstanceOf[ExecuteQueryResponse.Success]
        .outputWatermark shouldEqual Some(ts(3).toInstant)

      val actual = ParquetHelpers
        .read[StocksOwnedWithValue](outputDataPath)
        .sortBy(i => (i.event_time.getTime, i.symbol))

      actual shouldEqual List(
        StocksOwnedWithValue(ts(10), ts(2), "A", 100, 10, 1000),
        StocksOwnedWithValue(ts(10), ts(3), "A", 100, 12, 1200),
        StocksOwnedWithValue(ts(10), ts(3), "B", 200, 22, 4400)
      )
    }
  }

  test("AS OF join") {
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
          Ticker(0, ts(5), ts(1), "A", 10),
          Ticker(1, ts(5), ts(1), "B", 20),
          Ticker(2, ts(5), ts(2), "A", 10),
          Ticker(3, ts(5), ts(2), "B", 20),
          Ticker(4, ts(5), ts(3), "A", 12),
          Ticker(5, ts(5), ts(3), "B", 22),
          Ticker(6, ts(5), ts(4), "A", 14),
          Ticker(7, ts(5), ts(4), "B", 24)
        )
      )

      val stocksDataPath = tempDir.resolve("stocks.owned")
      ParquetHelpers.write(
        stocksDataPath,
        Seq(
          StocksOwned(0, ts(4), ts(2), "A", 100),
          StocksOwned(1, ts(4), ts(3), "B", 200)
        )
      )

      val checkpointPath = tempDir.resolve("checkpoint")
      File(checkpointPath).createDirectories()
      val outputDataPath = tempDir.resolve("output")

      val response = engine.executeRequest(
        ExecuteQueryRequest(
          datasetID = DatasetID(""),
          datasetName = DatasetName("output"),
          systemTime = ts(10).toInstant,
          offset = 0,
          vocab = DatasetVocabulary(),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            query = Some(
              """
                |SELECT
                |  t.event_time,
                |  t.symbol,
                |  owned.volume as volume,
                |  t.price as current_price,
                |  owned.volume * t.price as current_value
                |FROM `tickers` as t
                |JOIN `stocks.owned` FOR SYSTEM_TIME AS OF t.event_time as owned
                |ON t.symbol = owned.symbol
                |""".stripMargin
            ),
            queries = None,
            temporalTables = Some(
              Vector(
                TemporalTable(
                  name = "stocks.owned",
                  primaryKey = Vector("symbol")
                )
              )
            )
          ),
          inputs = Vector(
            ExecuteQueryInput(
              datasetID = DatasetID(""),
              datasetName = DatasetName("tickers"),
              vocab = DatasetVocabulary().withDefaults(),
              dataInterval = Some(OffsetInterval(start = 0, end = 7)),
              dataPaths = Vector(tickersDataPath),
              schemaFile = tickersDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(4).toInstant))
            ),
            ExecuteQueryInput(
              datasetID = DatasetID(""),
              datasetName = DatasetName("stocks.owned"),
              vocab = DatasetVocabulary().withDefaults(),
              dataInterval = Some(OffsetInterval(start = 0, end = 1)),
              dataPaths = Vector(stocksDataPath),
              schemaFile = stocksDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(3).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          outDataPath = outputDataPath
        )
      )

      response
        .asInstanceOf[ExecuteQueryResponse.Success]
        .outputWatermark shouldEqual Some(ts(3).toInstant)

      val actual = ParquetHelpers
        .read[StocksOwnedWithValue](outputDataPath)
        .sortBy(i => (i.event_time.getTime, i.symbol))

      actual shouldEqual List(
        StocksOwnedWithValue(ts(10), ts(2), "A", 100, 10, 1000),
        StocksOwnedWithValue(ts(10), ts(3), "A", 100, 12, 1200),
        StocksOwnedWithValue(ts(10), ts(3), "B", 200, 22, 4400)
      )
    }
  }
}
