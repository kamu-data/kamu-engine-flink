package dev.kamu.engine.flink.test

import java.sql.Timestamp

import dev.kamu.engine.flink.MaxOutOfOrderWatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration

class JoinStreamToTemporalTableTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Temporal table join") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)

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
        new MaxOutOfOrderWatermarkStrategy[(Timestamp, String, Long)](
          _._1.getTime,
          duration.Duration(1, duration.DAYS)
        )
      )
      .toTable(tEnv, 'event_time.rowtime, 'symbol, 'price)

    val transactions = env
      .fromCollection(transactionsData)
      .assignTimestampsAndWatermarks(
        new MaxOutOfOrderWatermarkStrategy[(Timestamp, String, Long)](
          _._1.getTime,
          duration.Duration(1, duration.DAYS)
        )
      )
      .toTable(tEnv, 'event_time.rowtime, 'symbol, 'volume)

    tEnv.createTemporarySystemFunction(
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
    query.toDataStream[Row](classOf[Row]).addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "+I[2000-01-01T00:00, A, 100, -1000]",
      "+I[2000-01-02T00:00, B, 100, -13000]",
      "+I[2000-01-05T00:00, A, -100, 900]",
      "+I[2000-01-05T00:00, B, 100, -11000]"
    ).sorted

    expected shouldEqual actual
  }
}
