package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dev.kamu.engine.flink.BoundedOutOfOrderWatermark
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration

class ProjectionTest extends FunSuite with Matchers with BeforeAndAfter {

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  ignore("AS OF projection") {
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

    tEnv.registerFunction(
      "Tickers",
      tickers
        .createTemporalTableFunction('event_time, 'symbol)
    )

    val query =
      tEnv.sqlQuery(
        """
      SELECT
        *
      FROM Tickers FOR SYSTEM_TIME AS OF TIMESTAMP '2000-01-02 01:00:00'
    """
      )

    val sink = StreamSink.stringSink()
    //query.toAppendStream[Row].addSink(sink)
    query.toAppendStream[Row].print()
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      ""
    ).sorted

    expected shouldEqual actual
  }
}
