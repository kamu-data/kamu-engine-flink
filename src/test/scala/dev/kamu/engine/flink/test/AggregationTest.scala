package dev.kamu.engine.flink.test

import java.sql.Timestamp
import dev.kamu.engine.flink.MaxOutOfOrderWatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class AggregationTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Tumbling window aggregation") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)

    val tickersData = Seq(
      (ts(1, 1), "A", 10L),
      (ts(1, 1), "B", 20L),
      (ts(1, 2), "A", 11L),
      (ts(1, 2), "B", 21L),
      (ts(2, 1), "A", 12L),
      (ts(2, 1), "B", 21L),
      (ts(2, 2), "A", 13L),
      (ts(2, 2), "B", 20L),
      (ts(1, 3), "A", 13L), // One day late and will be considered
      (ts(3, 1), "A", 10L),
      (ts(3, 1), "B", 19L),
      (ts(3, 2), "A", 9L),
      (ts(3, 2), "B", 18L),
      (ts(1, 4), "A", 14L) // Two days late and will be discarded
    )

    // DOC: https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html
    // DOC: https://stackoverflow.com/questions/55392857/why-flink-does-not-drop-late-data
    val tickers = env
      .fromCollection(tickersData)
      .assignTimestampsAndWatermarks(
        new MaxOutOfOrderWatermarkStrategy[(Timestamp, String, Long)](
          t => t._1.getTime,
          scala.concurrent.duration
            .Duration(1, scala.concurrent.duration.DAYS)
        )
      )
      .toTable(tEnv, 'event_time.rowtime, 'symbol, 'price)

    tEnv.createTemporaryView("Tickers", tickers)

    val query =
      tEnv.sqlQuery(
        """
      SELECT
        CAST(TUMBLE_START(event_time, INTERVAL '1' DAY) as DATE) as event_time,
        symbol as symbol,
        min(price) as `min`,
        max(price) as `max`
      FROM Tickers
      GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
    """
      )

    val sink = StreamSink.stringSink()
    query.toDataStream[Row](classOf[Row]).addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "+I[2000-01-01, A, 10, 13]",
      "+I[2000-01-01, B, 20, 21]",
      "+I[2000-01-02, A, 12, 13]",
      "+I[2000-01-02, B, 20, 21]",
      "+I[2000-01-03, A, 9, 10]",
      "+I[2000-01-03, B, 18, 19]"
    ).sorted

    expected shouldEqual actual
  }
}
