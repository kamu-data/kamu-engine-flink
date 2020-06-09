package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.DatasetLayout
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class StocksOwned(
  event_time: Timestamp,
  symbol: String,
  volume: Int
)

case class StocksOwnedWithValue(
  event_time: Timestamp,
  symbol: String,
  volume: Int,
  current_price: Int,
  current_value: Int
)

class EngineJoinStreamToTemporalTableTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter {

  val fileSystem = FileSystem.get(new Configuration())

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName),
      cacheDir = workspaceDir.resolve("cache", datasetName)
    )
  }

  test("Temporal table join") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val engineRunner =
        new EngineRunner(fileSystem, new DockerClient(fileSystem))

      val tickersLayout = tempLayout(tempDir, "tickers")
      val stocksOwnedLayout = tempLayout(tempDir, "stocks.owned")
      val currentValueLayout = tempLayout(tempDir, "value")

      val request = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: stocks.current-value
           |source:
           |  inputs:
           |    - id: tickers
           |    - id: stocks.owned
           |  transform:
           |    engine: flink
           |    watermarks:
           |    - id: tickers
           |      eventTimeColumn: event_time
           |    - id: stocks.owned
           |      eventTimeColumn: event_time
           |      primaryKey:
           |      - symbol
           |    query: >
           |      SELECT
           |        t.event_time, 
           |        t.symbol,
           |        owned.volume as volume,
           |        t.price as current_price,
           |        owned.volume * t.price as current_value
           |      FROM
           |        tickers as t,
           |        LATERAL TABLE (`stocks.owned`(t.event_time)) AS owned
           |      WHERE t.symbol = owned.symbol
           |inputSlices:
           |  tickers:
           |    hash: ""
           |    interval: "(-inf, inf)"
           |    numRecords: 0
           |  stocks.owned:
           |    hash: ""
           |    interval: "(-inf, inf)"
           |    numRecords: 0
           |datasetLayouts:
           |  tickers:
           |    metadataDir: /none
           |    dataDir: ${tickersLayout.dataDir}
           |    checkpointsDir: /none
           |    cacheDir: /none
           |  stocks.owned:
           |    metadataDir: /none
           |    dataDir: ${stocksOwnedLayout.dataDir}
           |    checkpointsDir: /none
           |    cacheDir: /none
           |  stocks.current-value:
           |    metadataDir: /none
           |    dataDir: ${currentValueLayout.dataDir}
           |    checkpointsDir: ${currentValueLayout.checkpointsDir}
           |    cacheDir: /none
           |datasetVocabs:
           |  tickers:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |  stocks.owned:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |  stocks.current-value:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |""".stripMargin
      )

      {
        ParquetHelpers.write(
          tickersLayout.dataDir.resolve("1.parquet"),
          Seq(
            Ticker(ts(1), "A", 10),
            Ticker(ts(1), "B", 20),
            Ticker(ts(2), "A", 12),
            Ticker(ts(2), "B", 22),
            Ticker(ts(3), "A", 14),
            Ticker(ts(3), "B", 24)
          )
        )

        ParquetHelpers.write(
          stocksOwnedLayout.dataDir.resolve("1.parquet"),
          Seq(
            StocksOwned(ts(1), "A", 100),
            StocksOwned(ts(2), "B", 200)
          )
        )

        val result = engineRunner.run(request)

        println(result.block)

        val actual = ParquetHelpers
          .read[StocksOwnedWithValue](
            currentValueLayout.dataDir.resolve(result.dataFileName.get)
          )
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          StocksOwnedWithValue(ts(1), "A", 100, 10, 1000),
          StocksOwnedWithValue(ts(2), "A", 100, 12, 1200)
          //StocksOwnedWithValue(ts(2), "B", 200, 22, 4400) ????
        )
      }

      {
        ParquetHelpers.write(
          tickersLayout.dataDir.resolve("2.parquet"),
          Seq(
            Ticker(ts(4), "A", 15),
            Ticker(ts(4), "B", 25)
          )
        )

        ParquetHelpers.write(
          stocksOwnedLayout.dataDir.resolve("2.parquet"),
          Seq(
            StocksOwned(ts(3), "B", 250)
          )
        )

        val result = engineRunner.run(request)

        println(result.block)

        val actual = ParquetHelpers
          .read[StocksOwnedWithValue](
            currentValueLayout.dataDir.resolve(result.dataFileName.get)
          )
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          StocksOwnedWithValue(ts(2), "B", 200, 22, 4400), // !!!???
          StocksOwnedWithValue(ts(3), "A", 100, 14, 1400),
          StocksOwnedWithValue(ts(3), "B", 250, 24, 6000)
        )
      }
    }
  }
}
