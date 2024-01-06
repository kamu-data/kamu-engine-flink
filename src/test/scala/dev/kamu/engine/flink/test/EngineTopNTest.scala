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
import dev.kamu.engine.flink.Op
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class EngineTopNTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Top-N") {
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
           |      cast(now() as timestamp(3)) as placed_at,
           |      *
           |    from (
           |      select
           |        row_number() over (order by score desc) AS place,
           |        match_time,
           |        player_name,
           |        score
           |      from match_scores
           |    ) where place <= 2
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  systemTimeColumn: system_time
           |  eventTimeColumn: placed_at
           |  operationTypeColumn: op
           |""".stripMargin
      )

      val checkpointDir = {
        var request = withRandomOutputPath(requestTemplate, outputLayout)
        request = withInputData(
          request,
          "match_scores",
          inputLayout.dataDir,
          Seq(
            // TODO: Test with retractions corrections in the input
            MatchScore(0, Op.Append, ts(5), ts(1), "Alice", 100),
            MatchScore(1, Op.Append, ts(5), ts(1), "Bob", 80),
            MatchScore(2, Op.Append, ts(5), ts(2), "Alice", 70),
            MatchScore(3, Op.Append, ts(5), ts(2), "Charlie", 90),
            MatchScore(4, Op.Append, ts(5), ts(3), "Bob", 60),
            MatchScore(5, Op.Append, ts(5), ts(3), "Charlie", 110)
          ),
          DatasetVocabulary.default().copy(eventTimeColumn = "match_time")
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("match_scores" -> ts(3)))
            .copy(systemTime = ts(10).toInstant, nextOffset = 0),
          tempDir
        )

        // TODO: Use actual `placed_at` once we figure out how to propagate `event_time` correctly
        val any = ts(9, 9, 9)

        val actual = ParquetHelpers
          .read[LeaderboardRow](request.newDataPath)
          .map(r => r.copy(placed_at = any))

        actual shouldEqual List(
          LeaderboardRow(0, Op.Append, ts(10), any, 1, ts(1), "Alice", 100),
          LeaderboardRow(1, Op.Append, ts(10), any, 2, ts(1), "Bob", 80),
          LeaderboardRow(2, Op.CorrectFrom, ts(10), any, 2, ts(1), "Bob", 80),
          LeaderboardRow(3, Op.CorrectTo, ts(10), any, 2, ts(2), "Charlie", 90),
          LeaderboardRow(
            4,
            Op.CorrectFrom,
            ts(10),
            any,
            1,
            ts(1),
            "Alice",
            100
          ),
          LeaderboardRow(
            5,
            Op.CorrectTo,
            ts(10),
            any,
            1,
            ts(3),
            "Charlie",
            110
          ),
          LeaderboardRow(
            6,
            Op.CorrectFrom,
            ts(10),
            any,
            2,
            ts(2),
            "Charlie",
            90
          ),
          LeaderboardRow(7, Op.CorrectTo, ts(10), any, 2, ts(1), "Alice", 100)
        )

        ParquetHelpers
          .getSchemaFromFile(request.newDataPath)
          .toString
          .trim() shouldEqual
          """message org.apache.flink.avro.generated.record {
            |  required int64 offset;
            |  required int32 op;
            |  required int64 system_time (TIMESTAMP(MILLIS,true));
            |  required int64 placed_at (TIMESTAMP(MILLIS,true));
            |  required int64 place;
            |  required int64 match_time (TIMESTAMP(MILLIS,true));
            |  required binary player_name (STRING);
            |  required int32 score;
            |}""".stripMargin

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 7
        )
        result.newWatermark.get shouldEqual ts(3).toInstant

        request.newCheckpointPath
      }
    }
  }
}
