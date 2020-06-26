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

class JoinStreamToTemporalTableTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter {

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  test("Temporal table join") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tickersData = Seq(
      (ts(1), "A", 10L),
      (ts(1), "B", 120L),
      (ts(2), "A", 11L),
      (ts(2), "B", 130L),
      (ts(3), "A", 12L),
      (ts(3), "B", 110L),
      (ts(4), "A", 15L),
      (ts(4), "B", 100L),
      (ts(5), "A", 9L),
      (ts(5), "B", 110L),
      (ts(6), "A", 13L),
      (ts(6), "B", 110L)
    )

    // TODO: Figure out why out of order data is not discarded
    val transactionsData = Seq(
      (ts(1), "A", 100L),
      (ts(2), "B", 100L),
      (ts(5), "A", -100L),
      (ts(5), "B", 100L)
    )

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

    val transactions = env
      .fromCollection(transactionsData)
      .assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, String, Long)](
            0,
            duration.Duration(1, duration.DAYS)
          )
      )
      .toTable(tEnv, 'event_time.rowtime, 'symbol, 'volume)

    tEnv.registerFunction(
      "Tickers",
      tickers
        .createTemporalTableFunction('event_time, 'symbol)
    )

    tEnv.createTemporaryView("Transactions", transactions)

    val query =
      tEnv.sqlQuery(
        """
      SELECT
        tr.event_time, 
        tr.symbol, 
        tr.volume, 
        -tr.volume * ct.price AS delta
      FROM 
        Transactions AS tr,
        LATERAL TABLE (Tickers(tr.event_time)) AS ct
      WHERE tr.symbol = ct.symbol
    """
      )

    val sink = StreamSink.stringSink()
    query.toAppendStream[Row].addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "2000-01-01T00:00,A,100,-1000",
      "2000-01-02T00:00,B,100,-13000",
      "2000-01-05T00:00,A,-100,900",
      "2000-01-05T00:00,B,100,-11000"
      //"2000-01-01 00:00:00.0,A,100,-1000",
      //"2000-01-02 00:00:00.0,B,100,-13000",
      //"2000-01-05 00:00:00.0,A,-100,900",
      //"2000-01-05 00:00:00.0,B,100,-11000"
    ).sorted

    expected shouldEqual actual
  }
}
