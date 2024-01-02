package dev.kamu.engine.flink.test

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.RawQueryRequest
import dev.kamu.core.utils.{DockerClient, Temp}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class EngineRawQueryTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Simple query") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val inputPath = tempDir.resolve("input.parquet")
      val outputPath = tempDir.resolve("output.parquet")

      ParquetHelpers.write(
        inputPath,
        Seq(
          Ticker(0, ts(5), ts(1), "A", 10),
          Ticker(1, ts(5), ts(2), "B", 20),
          Ticker(2, ts(5), ts(3), "A", 11),
          Ticker(3, ts(5), ts(4), "B", 21)
        )
      )

      val request = yaml.load[RawQueryRequest](
        s"""
           |inputDataPaths:
           |- ${inputPath}
           |transform:
           |  kind: Sql
           |  engine: flink
           |  queries:
           |  - query: |
           |      SELECT
           |        `offset`,
           |        `system_time`,
           |        `event_time`,
           |        `symbol`,
           |        `price` * 10 as `price`
           |      FROM input
           |outputDataPath: ${outputPath}
           |""".stripMargin
      )

      val result = engineRunner.executeRawQuery(
        request,
        tempDir
      )

      result.numRecords shouldEqual 4

      val actual = ParquetHelpers
        .read[Ticker](request.outputDataPath)
        .sortBy(_.offset)

      actual shouldEqual List(
        Ticker(0, ts(5), ts(1), "A", 100),
        Ticker(1, ts(5), ts(2), "B", 200),
        Ticker(2, ts(5), ts(3), "A", 110),
        Ticker(3, ts(5), ts(4), "B", 210)
      )
    }
  }
}
