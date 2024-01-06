package dev.kamu.engine.flink.test

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{
  DatasetVocabulary,
  OffsetInterval,
  TransformRequest
}
import dev.kamu.core.utils.{DockerClient, Temp}
import dev.kamu.core.utils.fs._
import dev.kamu.engine.flink.Op
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class EngineMapTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Map - simple") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[TransformRequest](
        s"""
           |datasetId: "did:odf:blah"
           |datasetAlias: out
           |systemTime: "2020-01-01T00:00:00Z"
           |nextOffset: 0
           |transform:
           |  kind: Sql
           |  engine: flink
           |  query: |
           |    SELECT
           |      event_time,
           |      symbol,
           |      price * 10 as price
           |    FROM `in`
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  systemTimeColumn: system_time
           |  eventTimeColumn: event_time
           |  operationTypeColumn: op
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

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> ts(4)))
            .copy(systemTime = ts(10).toInstant, nextOffset = 0),
          tempDir
        )

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 3
        )
        result.newWatermark.get shouldEqual ts(4).toInstant

        val actual = ParquetHelpers
          .read[Ticker](request.newDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          Ticker(0, ts(10), ts(1), "A", 100),
          Ticker(1, ts(10), ts(2), "B", 200),
          Ticker(2, ts(10), ts(3), "A", 110),
          Ticker(3, ts(10), ts(4), "B", 210)
        )

        request.newCheckpointPath
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

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> ts(8)))
            .copy(systemTime = ts(20).toInstant, nextOffset = 4),
          tempDir
        )

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 4,
          end = 7
        )
        result.newWatermark.get shouldEqual ts(8).toInstant

        val actual = ParquetHelpers
          .read[Ticker](request.newDataPath)
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

  test("Map - with corrections and retractions") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[TransformRequest](
        s"""
           |datasetId: "did:odf:blah"
           |datasetAlias: out
           |systemTime: "2020-01-01T00:00:00Z"
           |nextOffset: 0
           |transform:
           |  kind: Sql
           |  engine: flink
           |  query: |
           |    select
           |      op,
           |      event_time,
           |      symbol,
           |      price * 10 as price
           |    from input
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  systemTimeColumn: system_time
           |  eventTimeColumn: event_time
           |  operationTypeColumn: op
           |""".stripMargin
      )

      {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "input",
          inputLayout.dataDir,
          Seq(
            Ticker(0, Op.Append, ts(5), ts(1), "A", 10),
            Ticker(1, Op.CorrectFrom, ts(5), ts(1), "A", 10),
            Ticker(2, Op.CorrectTo, ts(5), ts(1), "A", 11),
            Ticker(3, Op.Retract, ts(5), ts(1), "A", 11)
          )
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("input" -> ts(1)))
            .copy(systemTime = ts(10).toInstant, nextOffset = 0),
          tempDir
        )

        val actual = ParquetHelpers
          .read[Ticker](request.newDataPath)

        actual shouldEqual List(
          Ticker(0, Op.Append, ts(10), ts(1), "A", 100),
          Ticker(1, Op.CorrectFrom, ts(10), ts(1), "A", 100),
          Ticker(2, Op.CorrectTo, ts(10), ts(1), "A", 110),
          Ticker(3, Op.Retract, ts(10), ts(1), "A", 110)
        )

        ParquetHelpers
          .getSchemaFromFile(request.newDataPath)
          .toString
          .trim() shouldEqual
          """message org.apache.flink.avro.generated.record {
              |  required int64 offset;
              |  required int32 op;
              |  required int64 system_time (TIMESTAMP(MILLIS,true));
              |  required int64 event_time (TIMESTAMP(MILLIS,true));
              |  required binary symbol (STRING);
              |  required int32 price;
              |}""".stripMargin

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 3
        )
        result.newWatermark.get shouldEqual ts(1).toInstant
      }
    }
  }
}
