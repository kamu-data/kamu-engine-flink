package dev.kamu.engine.flink.test

import dev.kamu.engine.flink.MaxOutOfOrderWatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import java.sql.Timestamp
import scala.concurrent.duration

class TopNTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Top-N") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)

    val matchScoresData = Seq(
      (ts(1), "Alice", 100),
      (ts(1), "Bob", 80),
      (ts(2), "Alice", 70),
      (ts(2), "Charlie", 90),
      (ts(3), "Bob", 60),
      (ts(3), "Charlie", 110)
    )

    val matchScores = env
      .fromCollection(matchScoresData)
      .assignTimestampsAndWatermarks(
        new MaxOutOfOrderWatermarkStrategy[(Timestamp, String, Int)](
          _._1.getTime,
          duration.Duration.Zero
        )
      )
      .toTable(tEnv, 'match_time.rowtime, 'player, 'score)

    tEnv.createTemporaryView("match_scores", matchScores)

    // TODO: Figure out what the event_time of Top-N query should be
    // The main idea is that we should have something like `placed_at` column
    // that shows when a score was assigned its current place.
    // In other words:
    // - Upon +I - `placed_at` should be equal to that record's `match_time`
    // - Upon +U - `placed_at` should be equal to `match_time` of the record that is currently being processed and caused the correction
    val query =
      tEnv.sqlQuery(
        """
        select
          coalesce(current_watermark(match_time), match_time) as placed_at,
          *
        from (
          select
            row_number() over (order by score desc) AS place,
            match_time,
            player,
            score
          from match_scores
        ) where place <= 2
        """
      )

    val sink = StreamSink.stringSink()
    query.toChangelogStream.addSink(sink)
    env.execute()

    val actual = sink.collectStr()

    // TODO: Notice that upon `-U` the `placed_at` value is different from the value
    //  that record was previously added (+I / +U) with. This seems to be a bug with
    //  when Flink evaluates `current_watermark`.
    val expected = List(
      "+I[2000-01-01T00:00, 1, 2000-01-01T00:00, Alice, 100]",
      "+I[2000-01-01T00:00, 2, 2000-01-01T00:00, Bob, 80]",
      "-U[2000-01-02T00:00, 2, 2000-01-01T00:00, Bob, 80]",
      "+U[2000-01-02T00:00, 2, 2000-01-02T00:00, Charlie, 90]",
      "-U[2000-01-03T00:00, 1, 2000-01-01T00:00, Alice, 100]",
      "+U[2000-01-03T00:00, 1, 2000-01-03T00:00, Charlie, 110]",
      "-U[2000-01-03T00:00, 2, 2000-01-02T00:00, Charlie, 90]",
      "+U[2000-01-03T00:00, 2, 2000-01-01T00:00, Alice, 100]"
    )

    expected shouldEqual actual
  }
}
