package dev.kamu.engine.flink.test

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{ExecuteQueryRequest, OffsetInterval}
import dev.kamu.core.utils.{DockerClient, Temp}
import dev.kamu.core.utils.fs._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class EngineMapTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Simple map") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: out
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  query: >
           |    SELECT
           |      event_time,
           |      symbol,
           |      price * 10 as price
           |    FROM `in`
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab: {}
           |""".stripMargin
      )

      val checkpointDir = {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(0, ts(5), ts(1), "A", 10),
            Ticker(1, ts(5), ts(2), "B", 20),
            Ticker(2, ts(5), ts(3), "A", 11),
            Ticker(3, ts(5), ts(4), "B", 21)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(4)))
            .copy(systemTime = ts(10).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 3
        )
        result.outputWatermark.get shouldEqual ts(4).toInstant

        val actual = ParquetHelpers
          .read[Ticker](request.outDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          Ticker(0, ts(10), ts(1), "A", 100),
          Ticker(1, ts(10), ts(2), "B", 200),
          Ticker(2, ts(10), ts(3), "A", 110),
          Ticker(3, ts(10), ts(4), "B", 210)
        )

        request.newCheckpointDir
      }

      { // Input comes in two files
        var request = withRandomOutputPath(
          requestTemplate,
          outputLayout,
          Some(checkpointDir)
        )

        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(4, ts(15), ts(5), "A", 12),
            Ticker(5, ts(15), ts(6), "B", 22)
          )
        )

        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(6, ts(15), ts(7), "A", 13),
            Ticker(7, ts(15), ts(8), "B", 23)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(8)))
            .copy(systemTime = ts(20).toInstant, offset = 4),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 4,
          end = 7
        )
        result.outputWatermark.get shouldEqual ts(8).toInstant

        val actual = ParquetHelpers
          .read[Ticker](request.outDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          Ticker(4, ts(20), ts(5), "A", 120),
          Ticker(5, ts(20), ts(6), "B", 220),
          Ticker(6, ts(20), ts(7), "A", 130),
          Ticker(7, ts(20), ts(8), "B", 230)
        )
      }
    }
  }
}
