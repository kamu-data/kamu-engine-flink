package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.DatasetLayout
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class Order(
  event_time: Timestamp,
  order_id: Int,
  quantity: Int
)

case class Shipment(
  event_time: Timestamp,
  order_id: Int,
  num_shipped: Int
)

case class ShippedOrder(
  order_time: Timestamp,
  order_id: Int,
  order_quantity: Int,
  shipped_time: Option[Timestamp],
  shipped_quantity: Option[Int]
)

class EngineJoinStreamToStreamTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter {

  val fileSystem = FileSystem.get(new Configuration())

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName),
      cacheDir = workspaceDir.resolve("cache", datasetName)
    )
  }

  test("Temporal table join") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val engineRunner =
        new EngineRunner(fileSystem, new DockerClient(fileSystem))

      val ordersLayout = tempLayout(tempDir, "orders")
      val shipmentsLayout = tempLayout(tempDir, "shipments")
      val shippedOrdersLayout = tempLayout(tempDir, "value")

      val request = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: shipped_orders
           |source:
           |  inputs:
           |    - id: orders
           |    - id: shipments
           |  transform:
           |    engine: flink
           |    watermarks:
           |    - id: orders
           |      eventTimeColumn: event_time
           |    - id: shipments
           |      eventTimeColumn: event_time
           |    query: >
           |      SELECT
           |        o.event_time as order_time,
           |        o.order_id,
           |        o.quantity as order_quantity,
           |        CAST(s.event_time as TIMESTAMP) as shipped_time,
           |        s.num_shipped as shipped_quantity
           |      FROM 
           |        orders as o
           |      LEFT JOIN shipments as s
           |      ON
           |        o.order_id = s.order_id 
           |        AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '2' DAY
           |inputSlices:
           |  orders:
           |    hash: ""
           |    interval: "(-inf, inf)"
           |    numRecords: 0
           |  shipments:
           |    hash: ""
           |    interval: "(-inf, inf)"
           |    numRecords: 0
           |datasetLayouts:
           |  orders:
           |    metadataDir: /none
           |    dataDir: ${ordersLayout.dataDir}
           |    checkpointsDir: /none
           |    cacheDir: /none
           |  shipments:
           |    metadataDir: /none
           |    dataDir: ${shipmentsLayout.dataDir}
           |    checkpointsDir: /none
           |    cacheDir: /none
           |  shipped_orders:
           |    metadataDir: /none
           |    dataDir: ${shippedOrdersLayout.dataDir}
           |    checkpointsDir: ${shippedOrdersLayout.checkpointsDir}
           |    cacheDir: /none
           |datasetVocabs:
           |  orders:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |  shipments:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |  shipped_orders:
           |    systemTimeColumn: system_time
           |    corruptRecordColumn: __corrupt_record__
           |""".stripMargin
      )

      {
        ParquetHelpers.write(
          ordersLayout.dataDir.resolve("1.parquet"),
          Seq(
            Order(ts(1), 1, 10),
            Order(ts(1), 2, 120),
            Order(ts(5), 3, 9)
          )
        )

        ParquetHelpers.write(
          shipmentsLayout.dataDir.resolve("1.parquet"),
          Seq(
            Shipment(ts(1), 1, 5),
            Shipment(ts(1), 1, 5),
            Shipment(ts(2), 2, 120)
          )
        )

        val result = engineRunner.run(request)

        println(result.block)

        val actual = ParquetHelpers
          .read[ShippedOrder](
            shippedOrdersLayout.dataDir.resolve(result.dataFileName.get)
          )
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShippedOrder(ts(1), 1, 10, Some(ts(1)), Some(5)),
          ShippedOrder(ts(1), 1, 10, Some(ts(1)), Some(5)),
          ShippedOrder(ts(1), 2, 120, Some(ts(2)), Some(120))
        )
      }

      {
        ParquetHelpers.write(
          ordersLayout.dataDir.resolve("2.parquet"),
          Seq(
            Order(ts(10), 4, 110)
          )
        )

        ParquetHelpers.write(
          shipmentsLayout.dataDir.resolve("2.parquet"),
          Seq(
            Shipment(ts(8), 3, 9),
            Shipment(ts(11), 4, 110)
          )
        )

        val result = engineRunner.run(request)

        println(result.block)

        val actual = ParquetHelpers
          .read[ShippedOrder](
            shippedOrdersLayout.dataDir.resolve(result.dataFileName.get)
          )
          .sortBy(i => (i.order_time.getTime, i.order_id))

        actual shouldEqual List(
          ShippedOrder(ts(5), 3, 9, None, None),
          ShippedOrder(ts(10), 4, 110, Some(ts(11)), Some(110))
        )
      }
    }
  }
}
