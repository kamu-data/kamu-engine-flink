package dev.kamu.engine.flink.test

import java.nio.file.Paths
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.ExecuteQueryRequest
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
           |transform:
           |  kind: sql
           |  engine: flink
           |  query: >
           |    SELECT * FROM `in`
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
          Ticker(ts(5), ts(1), "A", 10),
          Ticker(ts(5), ts(2), "B", 20),
          Ticker(ts(5), ts(3), "A", 11),
          Ticker(ts(5), ts(4), "B", 21)
        )
      )

      val result = engineRunner.run(
        withWatermarks(request, Map("in" -> ts(4))),
        tempDir,
        Timestamp.from(Instant.now)
      )

      result.metadataBlock.outputSlice.get.numRecords shouldEqual 4
      result.metadataBlock.outputSlice.get.hash shouldEqual "797199ca7c2073d724fbb27e72d0c14f6df032b44846c4aceec58c27dc3aed0c"
      result.metadataBlock.outputWatermark.get shouldEqual ts(4).toInstant

      val actual = ParquetHelpers
        .read[TickerNoSystemTime](request.outDataPath)
        .sortBy(i => (i.event_time.getTime, i.symbol))

      actual shouldEqual List(
        TickerNoSystemTime(ts(1), "A", 10),
        TickerNoSystemTime(ts(2), "B", 20),
        TickerNoSystemTime(ts(3), "A", 11),
        TickerNoSystemTime(ts(4), "B", 21)
      )
    }
  }
}
