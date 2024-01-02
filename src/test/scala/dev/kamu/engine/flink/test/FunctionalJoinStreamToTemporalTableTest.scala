package dev.kamu.engine.flink.test

import better.files.File
import dev.kamu.core.manifests._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.TransformEngine
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

      val engine = new TransformEngine(env, tEnv)

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

      val response = engine.executeTransform(
        TransformRequest(
          datasetId = DatasetId("did:odf:abcdef"),
          datasetAlias = DatasetAlias("output"),
          systemTime = ts(10).toInstant,
          nextOffset = 0,
          vocab = DatasetVocabulary.default(),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            queries = Some(
              Vector(
                SqlQueryStep(
                  None,
                  """
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
                  |""".stripMargin
                )
              )
            ),
            query = None,
            temporalTables = Some(
              Vector(
                TemporalTable(
                  name = "stocks.owned",
                  primaryKey = Vector("symbol")
                )
              )
            )
          ),
          queryInputs = Vector(
            TransformRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("tickers"),
              queryAlias = "tickers",
              vocab = DatasetVocabulary.default(),
              offsetInterval = Some(OffsetInterval(start = 0, end = 7)),
              dataPaths = Vector(tickersDataPath),
              schemaFile = tickersDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(4).toInstant))
            ),
            TransformRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("stocks.owned"),
              queryAlias = "stocks.owned",
              vocab = DatasetVocabulary.default(),
              offsetInterval = Some(OffsetInterval(start = 0, end = 1)),
              dataPaths = Vector(stocksDataPath),
              schemaFile = stocksDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(3).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          newDataPath = outputDataPath
        )
      )

      response
        .asInstanceOf[TransformResponse.Success]
        .newWatermark shouldEqual Some(ts(3).toInstant)

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

      val engine = new TransformEngine(env, tEnv)

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

      val response = engine.executeTransform(
        TransformRequest(
          datasetId = DatasetId("did:odf:abcdef"),
          datasetAlias = DatasetAlias("output"),
          systemTime = ts(10).toInstant,
          nextOffset = 0,
          vocab = DatasetVocabulary.default(),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            queries = Some(
              Vector(
                SqlQueryStep(
                  None,
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
                )
              )
            ),
            query = None,
            temporalTables = Some(
              Vector(
                TemporalTable(
                  name = "stocks.owned",
                  primaryKey = Vector("symbol")
                )
              )
            )
          ),
          queryInputs = Vector(
            TransformRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("tickers"),
              queryAlias = "tickers",
              vocab = DatasetVocabulary.default(),
              offsetInterval = Some(OffsetInterval(start = 0, end = 7)),
              dataPaths = Vector(tickersDataPath),
              schemaFile = tickersDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(4).toInstant))
            ),
            TransformRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("stocks.owned"),
              queryAlias = "stocks.owned",
              vocab = DatasetVocabulary.default(),
              offsetInterval = Some(OffsetInterval(start = 0, end = 1)),
              dataPaths = Vector(stocksDataPath),
              schemaFile = stocksDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(10).toInstant, ts(3).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          newDataPath = outputDataPath
        )
      )

      response
        .asInstanceOf[TransformResponse.Success]
        .newWatermark shouldEqual Some(ts(3).toInstant)

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
