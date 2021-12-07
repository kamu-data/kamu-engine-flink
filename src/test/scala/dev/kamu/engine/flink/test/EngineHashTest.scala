package dev.kamu.engine.flink.test

import java.nio.file.Paths
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{ExecuteQueryRequest, OffsetInterval}
import dev.kamu.core.utils.{DockerClient, Temp}
import dev.kamu.core.utils.fs._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.sql.Timestamp
import java.time.Instant

class EngineHashTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Hashing is stable") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "o.u.t")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: o.u.t
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  query: >
           |    SELECT event_time, symbol, price FROM `in`
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab: {}
           |""".stripMargin
      )

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
          .copy(systemTime = ts(10).toInstant),
        tempDir
      )

      result.metadataBlock.outputSlice.get.dataInterval shouldEqual OffsetInterval(
        start = 0,
        end = 3
      )
      result.metadataBlock.outputSlice.get.dataLogicalHash shouldEqual "0a148fa615c7c3c23022d333e02244a1fa7cde8e8255933a3ca8092284fc2cad"
      result.metadataBlock.outputWatermark.get shouldEqual ts(4).toInstant
    }
  }
}
