package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dev.kamu.engine.flink.BoundedOutOfOrderWatermark
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration

class JoinStreamToStreamTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  test("Stream to stream join") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ordersData = Seq(
      (ts(1), 1, 10L),
      (ts(1), 2, 120L),
      (ts(5), 3, 9L),
      (ts(10), 4, 110L)
    )

    val shipmentsData = Seq(
      (ts(1), 1, 5L),
      (ts(1), 1, 5L),
      (ts(2), 2, 120L),
      (ts(8), 3, 9L),
      (ts(11), 4, 110L)
    )

    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, Int, Long)](
            0,
            duration.Duration.Zero
          )
      )
      .toTable(tEnv, 'event_time.rowtime, 'order_id, 'quantity)

    val shipments = env
      .fromCollection(shipmentsData)
      .assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, Int, Long)](
            0,
            duration.Duration.Zero
          )
      )
      .toTable(tEnv, 'event_time.rowtime, 'order_id, 'num_shipped)

    tEnv.createTemporaryView("Orders", orders)
    tEnv.createTemporaryView("Shipments", shipments)

    val query =
      tEnv.sqlQuery(
        """
      SELECT
        CAST(o.event_time AS DATE) as order_time,
        o.order_id,
        o.quantity,
        CAST(s.event_time AS DATE) as shipped_time,
        s.num_shipped
      FROM 
        Orders as o
      LEFT JOIN Shipments as s
      ON 
        o.order_id = s.order_id 
        AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
    """
      )

    val sink = StreamSink.stringSink()
    query.toAppendStream[Row].addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "+I[2000-01-01, 1, 10, 2000-01-01, 5]",
      "+I[2000-01-01, 1, 10, 2000-01-01, 5]",
      "+I[2000-01-01, 2, 120, 2000-01-02, 120]",
      "+I[2000-01-05, 3, 9, null, null]",
      "+I[2000-01-10, 4, 110, 2000-01-11, 110]",
    ).sorted

    expected shouldEqual actual
  }

  test("Stream to stream join result can be used with other queries") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ordersData = Seq(
      (ts(1), 1, 10L),
      (ts(1), 2, 120L),
      (ts(5), 3, 9L),
      (ts(10), 4, 110L)
    )

    val shipmentsData = Seq(
      (ts(1), 1, 4L),
      (ts(2), 1, 6L),
      (ts(2), 2, 120L),
      (ts(8), 3, 9L),
      (ts(11), 4, 50L),
      (ts(13), 4, 60L)
    )

    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, Int, Long)](
            0,
            duration.Duration(1, duration.DAYS)
          )
      )
      .toTable(tEnv, 'event_time.rowtime, 'order_id, 'quantity)

    val shipments = env
      .fromCollection(shipmentsData)
      .assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark
          .forTuple[(Timestamp, Int, Long)](
            0,
            duration.Duration(1, duration.DAYS)
          )
      )
      .toTable(tEnv, 'event_time.rowtime, 'order_id, 'num_shipped)

    tEnv.createTemporaryView("Orders", orders)
    tEnv.createTemporaryView("Shipments", shipments)

    val orderShipments =
      tEnv.sqlQuery(
        """
      SELECT
        o.event_time as order_time,
        o.order_id,
        o.quantity as num_ordered,
        CAST(s.event_time AS DATE) as shipped_time,
        COALESCE(s.num_shipped, 0) as num_shipped
      FROM 
        Orders as o
      LEFT JOIN Shipments as s
      ON 
        o.order_id = s.order_id 
        AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
    """
      )
    tEnv.createTemporaryView("OrderShipments", orderShipments)

    orderShipments.toAppendStream[Row].print("orderShipments")

    val shipmentStats = tEnv.sqlQuery(
      """
         SELECT
           CAST(TUMBLE_START(order_time, INTERVAL '1' DAY) as DATE) as order_time,
           order_id,
           count(*) as num_shipments,
           min(shipped_time) as first_shipment,
           max(shipped_time) as last_shipment,
           min(num_ordered) as num_ordered,
           sum(num_shipped) as num_shipped_total
         FROM OrderShipments
         GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), order_id
      """
    )
    tEnv.createTemporaryView("ShipmentStats", shipmentStats)

    shipmentStats.toAppendStream[Row].print("shipmentStats")

    val query = tEnv.sqlQuery(
      """
         SELECT *
         FROM ShipmentStats
         WHERE num_ordered <> num_shipped_total
      """
    )

    val sink = StreamSink.stringSink()
    query.toAppendStream[Row].print("query")
    query.toAppendStream[Row].addSink(sink)
    env.execute()

    val actual = sink.collectStr().sorted

    val expected = List(
      "+I[2000-01-05, 3, 1, null, null, 9, 0]",
      "+I[2000-01-10, 4, 1, 2000-01-11, 2000-01-11, 110, 50]",
    ).sorted

    expected shouldEqual actual
  }
}
