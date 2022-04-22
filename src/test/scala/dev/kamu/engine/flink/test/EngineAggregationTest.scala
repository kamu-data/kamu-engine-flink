package dev.kamu.engine.flink.test

import java.nio.file.Files
import java.sql.Timestamp

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{ExecuteQueryRequest, ExecuteQueryInput}
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class Ticker(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  price: Int
) extends HasOffset {
  override def getOffset: Long = offset
}

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
    with TimeHelpers
    with EngineHelpers {

  test("Tumbling window aggregation - ordered") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: "did:odf:blah"
           |datasetName: out
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |vocab: {}
           |transform:
           |  kind: sql
           |  engine: flink
           |  query: >
           |    SELECT
           |      TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
           |      symbol as symbol,
           |      min(price) as `min`,
           |      max(price) as `max`
           |    FROM `in`
           |    GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
           |inputs: []
           |newCheckpointPath: ""
           |outDataPath: ""
           |""".stripMargin
      )

      var lastCheckpointDir = {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
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

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(3, 2)))
            .copy(systemTime = ts(10).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 3
        )
        result.outputWatermark.get shouldEqual ts(3, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(10), ts(1), "A", 10, 11),
          TickerSummary(ts(10), ts(1), "B", 20, 21),
          TickerSummary(ts(10), ts(2), "A", 12, 13),
          TickerSummary(ts(10), ts(2), "B", 22, 23)
        )

        request.newCheckpointPath
      }

      lastCheckpointDir = {
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(lastCheckpointDir)
        )
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(12, ts(15), ts(4, 1), "A", 16),
            Ticker(13, ts(15), ts(4, 1), "B", 26),
            Ticker(14, ts(15), ts(4, 2), "A", 17),
            Ticker(15, ts(15), ts(4, 2), "B", 27),
            Ticker(16, ts(15), ts(5, 1), "A", 18),
            Ticker(17, ts(15), ts(5, 1), "B", 28),
            Ticker(18, ts(15), ts(5, 2), "A", 19),
            Ticker(19, ts(15), ts(5, 2), "B", 29)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(5, 2)))
            .copy(systemTime = ts(20).toInstant, offset = 4),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 4,
          end = 7
        )
        result.outputWatermark.get shouldEqual ts(5, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(20), ts(3), "A", 14, 15),
          TickerSummary(ts(20), ts(3), "B", 24, 25),
          TickerSummary(ts(20), ts(4), "A", 16, 17),
          TickerSummary(ts(20), ts(4), "B", 26, 27)
        )

        request.newCheckpointPath
      }

      val (lastCheckpointDir2, lastInputFile) = {
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(lastCheckpointDir)
        )
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(20, ts(20), ts(6, 1), "A", 20),
            Ticker(21, ts(20), ts(6, 1), "B", 30)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(6, 1)))
            .copy(systemTime = ts(30).toInstant, offset = 12),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 12,
          end = 13
        )
        result.outputWatermark.get shouldEqual ts(6, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(30), ts(5), "A", 18, 19),
          TickerSummary(ts(30), ts(5), "B", 28, 29)
        )

        (
          request.newCheckpointPath,
          request.inputs
            .find(i => i.datasetName.toString == "in")
            .get
            .dataPaths(0)
        )
      }
      lastCheckpointDir = lastCheckpointDir2

      lastCheckpointDir = { // Advances watermark without new data
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(lastCheckpointDir)
        )
        request = request.copy(
          inputs = Vector(
            ExecuteQueryInput(
              datasetID = DatasetID("did:odf:abcd"),
              datasetName = DatasetName("in"),
              dataInterval = None,
              schemaFile = lastInputFile,
              dataPaths = Vector.empty,
              vocab = DatasetVocabulary(None, None),
              explicitWatermarks = Vector.empty
            )
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(7, 1)))
            .copy(systemTime = ts(31).toInstant, offset = 14),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 14,
          end = 15
        )
        result.outputWatermark.get shouldEqual ts(7, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(31), ts(6), "A", 20, 20),
          TickerSummary(ts(31), ts(6), "B", 30, 30)
        )

        request.newCheckpointPath
      }

      { // Advances watermark again without expecting any output this time
        // Verifying that previous watermark is propagated
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(lastCheckpointDir)
        )
        request = request.copy(
          inputs = Vector(
            ExecuteQueryInput(
              datasetID = DatasetID("did:odf:abcd"),
              datasetName = DatasetName("in"),
              dataInterval = None,
              schemaFile = lastInputFile,
              dataPaths = Vector.empty,
              vocab = DatasetVocabulary(None, None),
              explicitWatermarks = Vector.empty
            )
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(8)))
            .copy(systemTime = ts(31).toInstant, offset = 16),
          tempDir
        )

        result.dataInterval shouldBe None
        result.outputWatermark.get shouldEqual ts(8).toInstant

        assert(!Files.exists(request.outDataPath))
      }
    }
  }

  test("Tumbling window aggregation - late data") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
          |datasetID: "did:odf:blah"
          |datasetName: out
          |systemTime: "2020-01-01T00:00:00Z"
          |offset: 0
          |transform:
          |  kind: sql
          |  engine: flink
          |  query: >
          |    SELECT
          |      TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
          |      symbol as symbol,
          |      min(price) as `min`,
          |      max(price) as `max`
          |    FROM `in`
          |    GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
          |inputs: []
          |newCheckpointPath: ""
          |outDataPath: ""
          |vocab: {}
          |""".stripMargin
      )

      val lastCheckpointDir = {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(0, ts(5), ts(1, 1), "A", 10),
            Ticker(1, ts(5), ts(1, 1), "B", 20),
            Ticker(2, ts(5), ts(1, 2), "A", 10),
            Ticker(3, ts(5), ts(1, 2), "B", 21),
            Ticker(4, ts(5), ts(2, 1), "A", 12),
            Ticker(5, ts(5), ts(2, 1), "B", 22),
            Ticker(6, ts(5), ts(2, 2), "A", 13),
            Ticker(7, ts(5), ts(2, 2), "B", 23),
            Ticker(8, ts(5), ts(1, 3), "A", 11), // One day late and will be considered
            Ticker(9, ts(5), ts(3, 1), "A", 14),
            Ticker(10, ts(5), ts(3, 1), "B", 24),
            Ticker(11, ts(5), ts(3, 2), "A", 15),
            Ticker(12, ts(5), ts(3, 2), "B", 25)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(2, 2)))
            .copy(systemTime = ts(10).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 1
        )
        result.outputWatermark.get shouldEqual ts(2, 2).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(10), ts(1), "A", 10, 11),
          TickerSummary(ts(10), ts(1), "B", 20, 21)
        )

        request.newCheckpointPath
      }

      {
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(lastCheckpointDir)
        )
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(13, ts(10), ts(1, 4), "A", 12), // Two days late and will be discarded
            Ticker(14, ts(10), ts(4, 1), "A", 16),
            Ticker(15, ts(10), ts(4, 1), "B", 26),
            Ticker(16, ts(10), ts(4, 2), "A", 17),
            Ticker(17, ts(10), ts(4, 2), "B", 27),
            Ticker(18, ts(10), ts(5, 1), "A", 18),
            Ticker(19, ts(10), ts(5, 1), "B", 28)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(4, 1)))
            .copy(systemTime = ts(20).toInstant, offset = 2),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 2,
          end = 5
        )
        result.outputWatermark.get shouldEqual ts(4, 1).toInstant

        val actual = ParquetHelpers
          .read[TickerSummary](request.outDataPath)
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
