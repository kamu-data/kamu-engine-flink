package dev.kamu.engine.flink.test

import better.files.File
import dev.kamu.core.manifests._
import dev.kamu.core.utils.Temp
import dev.kamu.engine.flink.{Op, TransformEngine}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.sql.Timestamp
import java.time.ZoneId

case class MatchScore(
  offset: Long,
  op: Int,
  system_time: Timestamp,
  match_time: Timestamp,
  player_name: String,
  score: Int
) extends HasOffset {
  override def getOffset: Long = offset
}

case class LeaderboardRow(
  offset: Long,
  op: Int,
  system_time: Timestamp,
  placed_at: Timestamp,
  place: Long,
  match_time: Timestamp,
  player_name: String,
  score: Int
)

class FunctionalTopNTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Top-N") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tEnv = StreamTableEnvironment.create(env)
      tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

      env.setParallelism(1)

      val engine = new TransformEngine(env, tEnv)

      val inputDataPath = tempDir.resolve("match_scores")
      ParquetHelpers.write(
        inputDataPath,
        Seq(
          MatchScore(0, Op.Append, ts(5), ts(1), "Alice", 100),
          MatchScore(1, Op.Append, ts(5), ts(1), "Bob", 80),
          MatchScore(2, Op.Append, ts(5), ts(2), "Alice", 70),
          MatchScore(3, Op.Append, ts(5), ts(2), "Charlie", 90),
          MatchScore(4, Op.Append, ts(5), ts(3), "Bob", 60),
          MatchScore(5, Op.Append, ts(5), ts(3), "Charlie", 110)
        )
      )

      val checkpointPath = tempDir.resolve("checkpoint")
      File(checkpointPath).createDirectories()
      val outputDataPath = tempDir.resolve("output")

      engine.executeTransform(
        TransformRequest(
          datasetId = DatasetId("did:odf:abcdef"),
          datasetAlias = DatasetAlias("output"),
          systemTime = ts(10).toInstant,
          nextOffset = 0,
          // TODO: Figure out what the `event_time` of Top-N result should be.
          //
          //  The current best idea is that we should have something like `placed_at` column
          //  that shows when a score was assigned its current `place`.
          //
          //  In other words:
          //  - Upon +I `placed_at` should be equal to that record's `match_time`
          //  - Upon +U `placed_at` should be equal to `match_time` of the record that is currently being processed and caused the correction
          //
          //  The `coalesce(current_watermark(match_time), match_time) as placed_at` code we use in TopNTest
          //  doesn't work here because the `TransformEngine` does not currently derive watermarks on per-event basis
          //  but rather relies on explicit watermark mechanism where watermarks are injected at the end of the micro-batch.
          vocab =
            DatasetVocabulary.default().copy(eventTimeColumn = "placed_at"),
          transform = Transform.Sql(
            engine = "flink",
            version = None,
            queries = Some(
              Vector(
                SqlQueryStep(
                  None,
                  """
                  select
                    cast(now() as timestamp(3)) as placed_at,
                    *
                  from (
                    select
                      row_number() over (order by score desc) AS place,
                      match_time,
                      player_name,
                      score
                    from match_scores
                  ) where place <= 2
                  """
                )
              )
            ),
            query = None,
            temporalTables = None
          ),
          queryInputs = Vector(
            TransformRequestInput(
              datasetId = DatasetId("did:odf:abcdef"),
              datasetAlias = DatasetAlias("match_scores"),
              queryAlias = "match_scores",
              vocab = DatasetVocabulary
                .default()
                .copy(eventTimeColumn = "match_time"),
              offsetInterval = Some(OffsetInterval(start = 0, end = 5)),
              dataPaths = Vector(inputDataPath),
              schemaFile = inputDataPath,
              explicitWatermarks =
                Vector(Watermark(ts(5).toInstant, ts(3).toInstant))
            )
          ),
          prevCheckpointPath = None,
          newCheckpointPath = checkpointPath,
          newDataPath = outputDataPath
        )
      )

      // TODO: Use actual `placed_at` once we figure out how to propagate `event_time` correctly
      val any = ts(9, 9, 9)

      val actual = ParquetHelpers
        .read[LeaderboardRow](outputDataPath)
        .map(r => r.copy(placed_at = any))

      actual shouldEqual List(
        LeaderboardRow(0, Op.Append, ts(10), any, 1, ts(1), "Alice", 100),
        LeaderboardRow(1, Op.Append, ts(10), any, 2, ts(1), "Bob", 80),
        LeaderboardRow(2, Op.CorrectFrom, ts(10), any, 2, ts(1), "Bob", 80),
        LeaderboardRow(3, Op.CorrectTo, ts(10), any, 2, ts(2), "Charlie", 90),
        LeaderboardRow(4, Op.CorrectFrom, ts(10), any, 1, ts(1), "Alice", 100),
        LeaderboardRow(5, Op.CorrectTo, ts(10), any, 1, ts(3), "Charlie", 110),
        LeaderboardRow(6, Op.CorrectFrom, ts(10), any, 2, ts(2), "Charlie", 90),
        LeaderboardRow(7, Op.CorrectTo, ts(10), any, 2, ts(1), "Alice", 100)
      )
    }
  }
}
