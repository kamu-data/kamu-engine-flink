package dev.kamu.engine.flink.test

import java.nio.file.Paths
import java.sql.Timestamp
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{ExecuteQueryRequest, OffsetInterval}
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class StocksOwned(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  volume: Int
) extends HasOffset {
  override def getOffset: Long = offset
}

case class StocksOwnedWithValue(
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  volume: Int,
  current_price: Int,
  current_value: Int
)

class EngineJoinStreamToTemporalTableTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Temporal table join") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val tickersLayout = tempLayout(tempDir, "tickers")
      val stocksOwnedLayout = tempLayout(tempDir, "stocks.owned")
      val currentValueLayout = tempLayout(tempDir, "value")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: stocks.current-value
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  temporalTables:
           |  - id: stocks.owned
           |    primaryKey:
           |    - symbol
           |  query: >
           |    SELECT
           |      t.event_time,
           |      t.symbol,
           |      owned.volume as volume,
           |      t.price as current_price,
           |      owned.volume * t.price as current_value
           |    FROM
           |      tickers as t,
           |      LATERAL TABLE (`stocks.owned`(t.event_time)) AS owned
           |    WHERE t.symbol = owned.symbol
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab: {}
           |""".stripMargin
      )

      val lastCheckpoint = {
        var request = withRandomOutputPath(requestTemplate, currentValueLayout)

        request = withInputData(
          request,
          "tickers",
          tickersLayout.dataDir,
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

        request = withInputData(
          request,
          "stocks.owned",
          stocksOwnedLayout.dataDir,
          Seq(
            StocksOwned(0, ts(4), ts(2), "A", 100),
            StocksOwned(1, ts(4), ts(3), "B", 200)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("tickers" -> ts(4), "stocks.owned" -> ts(3))
          ).copy(systemTime = ts(10).toInstant),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 2
        )
        result.outputWatermark.get shouldEqual ts(3).toInstant

        val actual = ParquetHelpers
          .read[StocksOwnedWithValue](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          StocksOwnedWithValue(ts(10), ts(2), "A", 100, 10, 1000),
          StocksOwnedWithValue(ts(10), ts(3), "A", 100, 12, 1200),
          StocksOwnedWithValue(ts(10), ts(3), "B", 200, 22, 4400)
        )

        request.newCheckpointDir
      }

      {
        var request =
          withRandomOutputPath(
            requestTemplate,
            currentValueLayout,
            Some(lastCheckpoint)
          )

        request = withInputData(
          request,
          "tickers",
          tickersLayout.dataDir,
          Seq(
            Ticker(8, ts(6), ts(5), "A", 15),
            Ticker(9, ts(6), ts(5), "B", 25)
          )
        )

        request = withInputData(
          request,
          "stocks.owned",
          stocksOwnedLayout.dataDir,
          Seq(
            StocksOwned(2, ts(5), ts(4), "B", 250)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("tickers" -> ts(5), "stocks.owned" -> ts(4))
          ).copy(systemTime = ts(20).toInstant, offset = 3),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 3,
          end = 4
        )
        result.outputWatermark.get shouldEqual ts(4).toInstant

        val actual = ParquetHelpers
          .read[StocksOwnedWithValue](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          StocksOwnedWithValue(ts(20), ts(4), "A", 100, 14, 1400),
          StocksOwnedWithValue(ts(20), ts(4), "B", 250, 24, 6000)
        )
      }
    }
  }

  test("Temporal table join with source watermark") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val tickersLayout = tempLayout(tempDir, "tickers")
      val stocksOwnedLayout = tempLayout(tempDir, "stocks.owned")
      val currentValueLayout = tempLayout(tempDir, "value")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: stocks.current-value
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  temporalTables:
           |  - id: stocks.owned
           |    primaryKey:
           |    - symbol
           |  query: >
           |    SELECT
           |      t.event_time,
           |      t.symbol,
           |      owned.volume as volume,
           |      t.price as current_price,
           |      owned.volume * t.price as current_value
           |    FROM
           |      tickers as t,
           |      LATERAL TABLE (`stocks.owned`(t.event_time)) AS owned
           |    WHERE t.symbol = owned.symbol
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab: {}
           |""".stripMargin
      )

      {
        var request = withRandomOutputPath(requestTemplate, currentValueLayout)

        request = withInputData(
          request,
          "tickers",
          tickersLayout.dataDir,
          Seq(
            Ticker(0, ts(6), ts(1), "A", 1),
            Ticker(1, ts(6), ts(2), "A", 2),
            Ticker(2, ts(6), ts(3), "A", 3),
            Ticker(3, ts(6), ts(4), "A", 4),
            Ticker(4, ts(6), ts(5), "A", 5)
          )
        )

        request = withInputData(
          request,
          "stocks.owned",
          stocksOwnedLayout.dataDir,
          Seq(
            StocksOwned(0, ts(4), ts(3), "A", 100)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("tickers" -> ts(5), "stocks.owned" -> ts(5))
          ).copy(systemTime = ts(10).toInstant),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 2
        )
        result.outputWatermark.get shouldEqual ts(5).toInstant

        val actual = ParquetHelpers
          .read[StocksOwnedWithValue](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          StocksOwnedWithValue(ts(10), ts(3), "A", 100, 3, 300),
          StocksOwnedWithValue(ts(10), ts(4), "A", 100, 4, 400),
          StocksOwnedWithValue(ts(10), ts(5), "A", 100, 5, 500)
        )
      }
    }
  }
}
