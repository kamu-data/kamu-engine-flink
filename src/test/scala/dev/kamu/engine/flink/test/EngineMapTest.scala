package dev.kamu.engine.flink.test

import java.nio.file.Paths

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
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
           |source:
           |  inputs:
           |    - in
           |  transform:
           |    kind: sql
           |    engine: flink
           |    query: >
           |      SELECT
           |        event_time,
           |        symbol,
           |        price * 10 as price
           |      FROM `in`
           |inputSlices: {}
           |newCheckpointDir: ""
           |outDataPath: ""
           |datasetVocabs:
           |  in: {}
           |  out: {}
           |""".stripMargin
      )

      val checkpointDir = {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(ts(5), ts(1), "A", 10),
            Ticker(ts(5), ts(2), "B", 20),
            Ticker(ts(5), ts(3), "A", 11),
            Ticker(ts(5), ts(4), "B", 21)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(4))),
          tempDir,
          ts(10)
        )

        result.block.outputSlice.get.numRecords shouldEqual 4
        result.block.outputWatermark.get shouldEqual ts(4).toInstant

        val actual = ParquetHelpers
          .read[Ticker](Paths.get(request.outDataPath))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          Ticker(ts(10), ts(1), "A", 100),
          Ticker(ts(10), ts(2), "B", 200),
          Ticker(ts(10), ts(3), "A", 110),
          Ticker(ts(10), ts(4), "B", 210)
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
            Ticker(ts(15), ts(5), "A", 12),
            Ticker(ts(15), ts(6), "B", 22)
          )
        )

        request = withInputData(
          request,
          "in",
          inputLayout.dataDir,
          Seq(
            Ticker(ts(15), ts(7), "A", 13),
            Ticker(ts(15), ts(8), "B", 23)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("in" -> ts(8))),
          tempDir,
          ts(20)
        )

        result.block.outputSlice.get.numRecords shouldEqual 4
        result.block.outputWatermark.get shouldEqual ts(8).toInstant

        val actual = ParquetHelpers
          .read[Ticker](Paths.get(request.outDataPath))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          Ticker(ts(20), ts(5), "A", 120),
          Ticker(ts(20), ts(6), "B", 220),
          Ticker(ts(20), ts(7), "A", 130),
          Ticker(ts(20), ts(8), "B", 230)
        )
      }
    }
  }
}
