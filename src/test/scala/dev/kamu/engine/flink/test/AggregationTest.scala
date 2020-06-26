package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dev.kamu.engine.flink.BoundedOutOfOrderWatermark
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration

class AggregationTest extends FunSuite with Matchers with BeforeAndAfter {

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  test("Tumbling window aggregation") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, String, Long)](
            0,
            duration.Duration(1, duration.DAYS)
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
    query.toAppendStream[Row].addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "2000-01-01,A,10,13",
      "2000-01-01,B,20,21",
      "2000-01-02,A,12,13",
      "2000-01-02,B,20,21",
      "2000-01-03,A,9,10",
      "2000-01-03,B,18,19"
    ).sorted

    expected shouldEqual actual
  }
}
