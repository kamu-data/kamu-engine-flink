package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class EngineMapTest extends FunSuite with Matchers with BeforeAndAfter {

  val fileSystem = FileSystem.get(new Configuration())

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  test("Simple map") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val engineRunner =
        new EngineRunner(fileSystem, new DockerClient(fileSystem))

      val inputDataDir = tempDir.resolve("data", "in")
      val outputDataDir = tempDir.resolve("data", "out")
      val outputCheckpointDir = tempDir.resolve("checkpoints", "out")

      val request = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: out
           |source:
           |  inputs:
           |    - id: in
           |  transform:
           |    engine: flink
           |    query: >
           |      SELECT
           |        event_time,
           |        symbol,
           |        price * 10 as price
           |      FROM `in`
           |inputSlices:
           |  in:
           |    hash: ""
           |    interval: "(-inf, inf)"
           |    numRecords: 0
           |datasetLayouts:
           |  in:
           |    metadataDir: /none
           |    dataDir: $inputDataDir
           |    checkpointsDir: /none
           |    cacheDir: /none
           |  out:
           |    metadataDir: /none
           |    dataDir: $outputDataDir
           |    checkpointsDir: $outputCheckpointDir
           |    cacheDir: /none
           |datasetVocabs:
           |  in: {}
           |  out: {}
           |""".stripMargin
      )

      {
        ParquetHelpers.write(
          inputDataDir.resolve("1.parquet"),
          Seq(
            Ticker(ts(5), ts(1), "A", 10),
            Ticker(ts(5), ts(2), "B", 20),
            Ticker(ts(5), ts(3), "A", 11),
            Ticker(ts(5), ts(4), "B", 21)
          )
        )

        val result = engineRunner.run(request, tempDir, ts(10))

        println(result.block)

        val actual = ParquetHelpers
          .read[Ticker](outputDataDir.resolve(result.dataFileName.get))
          .sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          Ticker(ts(10), ts(1), "A", 100),
          Ticker(ts(10), ts(2), "B", 200),
          Ticker(ts(10), ts(3), "A", 110),
          Ticker(ts(10), ts(4), "B", 210)
        )
      }

      {
        ParquetHelpers.write(
          inputDataDir.resolve("2.parquet"),
          Seq(
            Ticker(ts(15), ts(5), "A", 12),
            Ticker(ts(15), ts(6), "B", 22),
            Ticker(ts(15), ts(7), "A", 13),
            Ticker(ts(15), ts(8), "B", 23)
          )
        )

        val result = engineRunner.run(request, tempDir, ts(20))

        println(result.block)

        val actual = ParquetHelpers
          .read[Ticker](outputDataDir.resolve(result.dataFileName.get))
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
