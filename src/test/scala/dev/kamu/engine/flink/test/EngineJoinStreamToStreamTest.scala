package dev.kamu.engine.flink.test

import java.nio.file.Paths
import java.sql.Timestamp
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{ExecuteQueryRequest, OffsetInterval}
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class Order(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  order_id: Long,
  quantity: Long
) extends HasOffset {
  override def getOffset: Long = offset
}

case class Shipment(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  order_id: Long,
  num_shipped: Long
) extends HasOffset {
  override def getOffset: Long = offset
}

case class ShippedOrder(
  system_time: Timestamp,
  order_time: Timestamp,
  order_id: Long,
  order_quantity: Long,
  shipped_time: Option[Timestamp],
  shipped_quantity: Long
)

case class ShipmentStats(
  system_time: Timestamp,
  order_time: Timestamp,
  order_id: Long,
  num_shipments: Long,
  first_shipment: Option[Timestamp],
  last_shipment: Option[Timestamp],
  order_quantity: Long,
  shipped_quantity_total: Long
)

class EngineJoinStreamToStreamTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Stream to stream join") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val ordersLayout = tempLayout(tempDir, "orders")
      val shipmentsLayout = tempLayout(tempDir, "shipments")
      val shippedOrdersLayout = tempLayout(tempDir, "shipped_orders")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: "did:odf:blah"
           |datasetName: shipped_orders
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  query: >
           |    SELECT
           |      o.event_time as order_time,
           |      o.order_id,
           |      o.quantity as order_quantity,
           |      CAST(s.event_time as TIMESTAMP) as shipped_time,
           |      COALESCE(s.num_shipped, 0) as shipped_quantity
           |    FROM
           |      orders as o
           |    LEFT JOIN shipments as s
           |    ON
           |      o.order_id = s.order_id
           |      AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab:
           |  eventTimeColumn: order_time
           |""".stripMargin
      )

      val lastCheckpointDir = {
        var request = withRandomOutputPath(requestTemplate, shippedOrdersLayout)

        request = withInputData(
          request,
          "orders",
          ordersLayout.dataDir,
          Seq(
            Order(0, ts(6), ts(1), 1, 10),
            Order(1, ts(6), ts(1), 2, 120),
            Order(2, ts(6), ts(5), 3, 9)
          )
        )

        request = withInputData(
          request,
          "shipments",
          shipmentsLayout.dataDir,
          Seq(
            Shipment(0, ts(3), ts(1), 1, 4),
            Shipment(1, ts(3), ts(2), 1, 6),
            Shipment(2, ts(3), ts(2), 2, 120)
          )
        )

        val result = engineRunner.run(
          withWatermarks(request, Map("orders" -> ts(5), "shipments" -> ts(2)))
            .copy(systemTime = ts(10).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 2
        )

        val actual = ParquetHelpers
          .read[ShippedOrder](request.outDataPath)
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShippedOrder(ts(10), ts(1), 1, 10, Some(ts(1)), 4),
          ShippedOrder(ts(10), ts(1), 1, 10, Some(ts(2)), 6),
          ShippedOrder(ts(10), ts(1), 2, 120, Some(ts(2)), 120)
        )

        request.newCheckpointDir
      }

      {
        var request = withRandomOutputPath(
          requestTemplate,
          shippedOrdersLayout,
          Some(lastCheckpointDir)
        )

        request = withInputData(
          request,
          "orders",
          ordersLayout.dataDir,
          Seq(
            Order(3, ts(11), ts(10), 4, 110)
          )
        )

        request = withInputData(
          request,
          "shipments",
          shipmentsLayout.dataDir,
          Seq(
            Shipment(3, ts(12), ts(8), 3, 9),
            Shipment(4, ts(12), ts(11), 4, 110)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("orders" -> ts(10), "shipments" -> ts(11))
          ).copy(systemTime = ts(20).toInstant, offset = 3),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 3,
          end = 4
        )
        result.outputWatermark.get shouldEqual ts(8).toInstant

        val actual = ParquetHelpers
          .read[ShippedOrder](request.outDataPath)
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShippedOrder(ts(20), ts(5), 3, 9, None, 0),
          ShippedOrder(ts(20), ts(10), 4, 110, Some(ts(11)), 110)
        )
      }
    }
  }

  test("Stream to stream join result can be used with other queries") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val ordersLayout = tempLayout(tempDir, "orders")
      val shipmentsLayout = tempLayout(tempDir, "shipments")
      val lateOrdersLayout = tempLayout(tempDir, "late_orders")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: "did:odf:blah"
           |datasetName: late_orders
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  queries:
           |  - alias: order_shipments
           |    query: >
           |      SELECT
           |        o.event_time as order_time,
           |        o.order_id,
           |        o.quantity as order_quantity,
           |        CAST(s.event_time as TIMESTAMP) as shipped_time,
           |        COALESCE(s.num_shipped, 0) as shipped_quantity
           |      FROM
           |        orders as o
           |      LEFT JOIN shipments as s
           |      ON
           |        o.order_id = s.order_id
           |        AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
           |  - alias: shipment_stats
           |    query: >
           |      SELECT
           |        TUMBLE_START(order_time, INTERVAL '1' DAY) as order_time,
           |        order_id,
           |        count(*) as num_shipments,
           |        min(shipped_time) as first_shipment,
           |        max(shipped_time) as last_shipment,
           |        min(order_quantity) as order_quantity,
           |        sum(shipped_quantity) as shipped_quantity_total
           |      FROM order_shipments
           |      GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), order_id
           |  - alias: late_orders
           |    query: >
           |      SELECT *
           |      FROM shipment_stats
           |      WHERE order_quantity <> shipped_quantity_total
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab:
           |  eventTimeColumn: order_time
           |""".stripMargin
      )

      {
        var request = withRandomOutputPath(requestTemplate, lateOrdersLayout)

        request = withInputData(
          request,
          "orders",
          ordersLayout.dataDir,
          Seq(
            Order(0, ts(16), ts(1), 1, 10),
            Order(1, ts(16), ts(1), 2, 120),
            Order(2, ts(16), ts(5), 3, 9),
            Order(3, ts(16), ts(10), 4, 110),
            Order(4, ts(16), ts(15), 5, 10)
          )
        )

        request = withInputData(
          request,
          "shipments",
          shipmentsLayout.dataDir,
          Seq(
            Shipment(0, ts(17), ts(1), 1, 4),
            Shipment(1, ts(17), ts(2), 1, 6),
            Shipment(2, ts(17), ts(2), 2, 120),
            Shipment(3, ts(17), ts(6), 3, 5),
            Shipment(4, ts(17), ts(11), 4, 50),
            Shipment(5, ts(17), ts(13), 4, 60),
            Shipment(6, ts(17), ts(16), 5, 10)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("orders" -> ts(15), "shipments" -> ts(16))
          ).copy(systemTime = ts(20).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 1
        )
        result.outputWatermark.get shouldEqual ts(13).toInstant

        val actual = ParquetHelpers
          .read[ShipmentStats](request.outDataPath)
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShipmentStats(ts(20), ts(5), 3, 1, Some(ts(6)), Some(ts(6)), 9, 5),
          ShipmentStats(
            ts(20),
            ts(10),
            4,
            1,
            Some(ts(11)),
            Some(ts(11)),
            110,
            50
          )
        )
      }
    }
  }

  test("Stream to stream join result can be used with other queries (tricky)") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val ordersLayout = tempLayout(tempDir, "orders")
      val shipmentsLayout = tempLayout(tempDir, "shipments")
      val lateOrdersLayout = tempLayout(tempDir, "late_orders")

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: "did:odf:blah"
           |datasetName: late_orders
           |systemTime: "2020-01-01T00:00:00Z"
           |offset: 0
           |transform:
           |  kind: sql
           |  engine: flink
           |  queries:
           |  - alias: order_shipments
           |    query: >
           |      SELECT
           |        o.event_time as order_time,
           |        o.order_id,
           |        o.quantity as order_quantity,
           |        CAST(s.event_time as TIMESTAMP) as shipped_time,
           |        COALESCE(s.num_shipped, 0) as shipped_quantity
           |      FROM
           |        orders as o
           |      LEFT JOIN shipments as s
           |      ON
           |        o.order_id = s.order_id
           |        AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
           |  - alias: shipment_stats
           |    query: >
           |      SELECT
           |        TUMBLE_START(order_time, INTERVAL '1' DAY) as order_time,
           |        order_id,
           |        count(*) as num_shipments,
           |        min(shipped_time) as first_shipment,
           |        max(shipped_time) as last_shipment,
           |        min(order_quantity) as order_quantity,
           |        sum(shipped_quantity) as shipped_quantity_total
           |      FROM order_shipments
           |      GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), order_id
           |  - alias: late_orders
           |    query: >
           |      SELECT *
           |      FROM shipment_stats
           |      WHERE order_quantity <> shipped_quantity_total
           |inputs: []
           |newCheckpointDir: ""
           |outDataPath: ""
           |vocab:
           |  eventTimeColumn: order_time
           |""".stripMargin
      )

      {
        var request = withRandomOutputPath(requestTemplate, lateOrdersLayout)

        request = withInputData(
          request,
          "orders",
          ordersLayout.dataDir,
          Seq(
            Order(0, ts(11), ts(1), 1, 10),
            Order(1, ts(11), ts(1), 2, 120),
            Order(2, ts(11), ts(5), 3, 9),
            Order(3, ts(11), ts(10), 4, 110)
          )
        )

        request = withInputData(
          request,
          "shipments",
          shipmentsLayout.dataDir,
          Seq(
            Shipment(0, ts(14), ts(1), 1, 4),
            Shipment(1, ts(14), ts(2), 1, 6),
            Shipment(2, ts(14), ts(2), 2, 120),
            Shipment(3, ts(14), ts(8), 3, 9),
            Shipment(4, ts(14), ts(11), 4, 50),
            Shipment(5, ts(14), ts(13), 4, 60)
          )
        )

        val result = engineRunner.run(
          withWatermarks(
            request,
            Map("orders" -> ts(13), "shipments" -> ts(13))
          ).copy(systemTime = ts(20).toInstant, offset = 0),
          tempDir
        )

        result.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 1
        )
        result.outputWatermark.get shouldEqual ts(11).toInstant

        val actual = ParquetHelpers
          .read[ShipmentStats](request.outDataPath)
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShipmentStats(ts(20), ts(5), 3, 1, None, None, 9, 0),
          ShipmentStats(
            ts(20),
            ts(10),
            4,
            1,
            Some(ts(11)),
            Some(ts(11)),
            110,
            50
          )
        )
      }
    }
  }
}
