package dev.kamu.engine.flink.test

import java.sql.Timestamp

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.{ExecuteQueryRequest, Watermark}
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import spire.math.Interval

case class Ticker(
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  price: Int
)

case class TickerSummary(
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  min: Int,
  max: Int
)

class EngineAggregationTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Tumbling window aggregation - ordered") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputDataDir = tempDir.resolve("data", "in")
      val outputDataDir = tempDir.resolve("data", "out")
      val outputCheckpointDir = tempDir.resolve("checkpoints", "out")

      val request = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: out
           |source:
           |  inputs:
           |    - in
           |  transform:
           |    kind: sql
           |    engine: flink
           |    query: >
           |      SELECT
           |        TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
           |        symbol as symbol,
           |        min(price) as `min`,
           |        max(price) as `max`
           |      FROM `in`
           |      GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
           |inputSlices:
           |  in:
           |    interval: "(-inf, inf)"
           |    explicitWatermarks: []
           |dataDirs:
           |  in: $inputDataDir
           |  out: $outputDataDir
           |checkpointsDir: $outputCheckpointDir
           |datasetVocabs:
           |  in: {}
           |  out: {}
           |""".stripMargin
      )

      {
        ParquetHelpers.write(
          inputDataDir.resolve("1.parquet"),
          Seq(
            Ticker(ts(5), ts(1, 1), "A", 10),
            Ticker(ts(5), ts(1, 1), "B", 20),
            Ticker(ts(5), ts(1, 2), "A", 11),
            Ticker(ts(5), ts(1, 2), "B", 21),
            Ticker(ts(5), ts(2, 1), "A", 12),
            Ticker(ts(5), ts(2, 1), "B", 22),
            Ticker(ts(5), ts(2, 2), "A", 13),
            Ticker(ts(5), ts(2, 2), "B", 23),
            Ticker(ts(5), ts(3, 1), "A", 14),
            Ticker(ts(5), ts(3, 1), "B", 24),
            Ticker(ts(5), ts(3, 2), "A", 15),
            Ticker(ts(5), ts(3, 2), "B", 25)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(3, 2))),
          tempDir,
          ts(10)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 12
        result.block.outputSlice.get.numRecords shouldEqual 4
        result.block.outputSlice.get.interval shouldEqual Interval.point(
          ts(10).toInstant
        )
        result.block.outputWatermark.get shouldEqual ts(3, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(10), ts(1), "A", 10, 11),
          TickerSummary(ts(10), ts(1), "B", 20, 21),
          TickerSummary(ts(10), ts(2), "A", 12, 13),
          TickerSummary(ts(10), ts(2), "B", 22, 23)
        )
      }

      {
        ParquetHelpers.write(
          inputDataDir.resolve("2.parquet"),
          Seq(
            Ticker(ts(15), ts(4, 1), "A", 16),
            Ticker(ts(15), ts(4, 1), "B", 26),
            Ticker(ts(15), ts(4, 2), "A", 17),
            Ticker(ts(15), ts(4, 2), "B", 27),
            Ticker(ts(15), ts(5, 1), "A", 18),
            Ticker(ts(15), ts(5, 1), "B", 28),
            Ticker(ts(15), ts(5, 2), "A", 19),
            Ticker(ts(15), ts(5, 2), "B", 29)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(5, 2))),
          tempDir,
          ts(20)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 8
        result.block.outputSlice.get.numRecords shouldEqual 4
        result.block.outputWatermark.get shouldEqual ts(5, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(20), ts(3), "A", 14, 15),
          TickerSummary(ts(20), ts(3), "B", 24, 25),
          TickerSummary(ts(20), ts(4), "A", 16, 17),
          TickerSummary(ts(20), ts(4), "B", 26, 27)
        )
      }

      {
        ParquetHelpers.write(
          inputDataDir.resolve("3.parquet"),
          Seq(
            Ticker(ts(20), ts(6, 1), "A", 20),
            Ticker(ts(20), ts(6, 1), "B", 30)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(6, 1))),
          tempDir,
          ts(30)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 2
        result.block.outputSlice.get.numRecords shouldEqual 2
        result.block.outputWatermark.get shouldEqual ts(6, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(30), ts(5), "A", 18, 19),
          TickerSummary(ts(30), ts(5), "B", 28, 29)
        )
      }

      { // Advances watermark without new data
        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(7, 1))),
          tempDir,
          ts(31)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 0
        result.block.outputSlice.get.numRecords shouldEqual 2
        result.block.outputWatermark.get shouldEqual ts(7, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(31), ts(6), "A", 20, 20),
          TickerSummary(ts(31), ts(6), "B", 30, 30)
        )
      }

      { // Advances watermark again without expecting any output this time
        // Verifying that previous watermark is propagated
        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(8))),
          tempDir,
          ts(31)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 0
        result.block.outputSlice.get.numRecords shouldEqual 0
        result.block.outputWatermark.get shouldEqual ts(8).toInstant
      }
    }
  }

  test("Tumbling window aggregation - late data") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputDataDir = tempDir.resolve("data", "in")
      val outputDataDir = tempDir.resolve("data", "out")
      val outputCheckpointDir = tempDir.resolve("checkpoints", "out")

      val request = yaml.load[ExecuteQueryRequest](
        s"""
          |datasetID: out
          |source:
          |  inputs:
          |    - in
          |  transform:
          |    kind: sql
          |    engine: flink
          |    query: >
          |      SELECT
          |        TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
          |        symbol as symbol,
          |        min(price) as `min`,
          |        max(price) as `max`
          |      FROM `in`
          |      GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
          |inputSlices:
          |  in:
          |    interval: "(-inf, inf)"
          |    explicitWatermarks: []
          |dataDirs:
          |  in: $inputDataDir
          |  out: $outputDataDir
          |checkpointsDir: $outputCheckpointDir
          |datasetVocabs:
          |  in: {}
          |  out: {}
          |""".stripMargin
      )

      {
        ParquetHelpers.write(
          inputDataDir.resolve("1.parquet"),
          Seq(
            Ticker(ts(5), ts(1, 1), "A", 10),
            Ticker(ts(5), ts(1, 1), "B", 20),
            Ticker(ts(5), ts(1, 2), "A", 10),
            Ticker(ts(5), ts(1, 2), "B", 21),
            Ticker(ts(5), ts(2, 1), "A", 12),
            Ticker(ts(5), ts(2, 1), "B", 22),
            Ticker(ts(5), ts(2, 2), "A", 13),
            Ticker(ts(5), ts(2, 2), "B", 23),
            Ticker(ts(5), ts(1, 3), "A", 11), // One day late and will be considered
            Ticker(ts(5), ts(3, 1), "A", 14),
            Ticker(ts(5), ts(3, 1), "B", 24),
            Ticker(ts(5), ts(3, 2), "A", 15),
            Ticker(ts(5), ts(3, 2), "B", 25)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(2, 2))),
          tempDir,
          ts(10)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 13
        result.block.outputSlice.get.numRecords shouldEqual 2
        result.block.outputWatermark.get shouldEqual ts(2, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(10), ts(1), "A", 10, 11),
          TickerSummary(ts(10), ts(1), "B", 20, 21)
        )
      }

      {
        ParquetHelpers.write(
          inputDataDir.resolve("2.parquet"),
          Seq(
            Ticker(ts(10), ts(1, 4), "A", 12), // Two days late and will be discarded
            Ticker(ts(10), ts(4, 1), "A", 16),
            Ticker(ts(10), ts(4, 1), "B", 26),
            Ticker(ts(10), ts(4, 2), "A", 17),
            Ticker(ts(10), ts(4, 2), "B", 27),
            Ticker(ts(10), ts(5, 1), "A", 18),
            Ticker(ts(10), ts(5, 1), "B", 28)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(4, 1))),
          tempDir,
          ts(20)
        )

        result.block.inputSlices.get.apply(0).numRecords shouldEqual 7
        result.block.outputSlice.get.numRecords shouldEqual 4
        result.block.outputWatermark.get shouldEqual ts(4, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(20), ts(2), "A", 12, 13),
          TickerSummary(ts(20), ts(2), "B", 22, 23),
          TickerSummary(ts(20), ts(3), "A", 14, 15),
          TickerSummary(ts(20), ts(3), "B", 24, 25)
        )
      }
    }
  }
}
