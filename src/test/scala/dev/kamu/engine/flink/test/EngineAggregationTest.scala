package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import com.sksamuel.avro4s._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import dev.kamu.core.manifests.infra.TransformConfig
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.DockerClient
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

case class Ticker(
  event_time: Timestamp,
  symbol: String,
  price: Int
)

case class TickerSummary(
  event_time: Timestamp,
  symbol: String,
  min: Int,
  max: Int
)

class EngineAggregationTest extends FunSuite with Matchers with BeforeAndAfter {

  val fileSystem = FileSystem.get(new Configuration())

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  def writeParquet[T: Encoder: Decoder](path: Path, data: Seq[T])(
    implicit schemaFor: SchemaFor[T]
  ): Unit = {
    val avroSchema = AvroSchema[T]
    val format = RecordFormat[T]

    val records = data.map(format.to)

    println(avroSchema.toString(true))

    val writer = AvroParquetWriter
      .builder[GenericRecord](path)
      .withSchema(avroSchema)
      .withDataModel(GenericData.get)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    records.foreach(writer.write)

    writer.close()
  }

  def readParquet[T: Encoder: Decoder](path: Path)(
    implicit schemaFor: SchemaFor[T]
  ): List[T] = {
    val format = RecordFormat[T]

    val reader =
      AvroParquetReader
        .builder[GenericRecord](path)
        .withDataModel(GenericData.get)
        .build()

    val records = Stream
      .continually(reader.read)
      .takeWhile(_ != null)
      .map(format.from)
      .toList

    reader.close()
    records
  }

  test("Tumbling window aggregation") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val engineRunner =
        new EngineRunner(fileSystem, new DockerClient(fileSystem))

      val inputDataDir = tempDir.resolve("data", "in")
      val outputDataDir = tempDir.resolve("data", "out")
      val outputCheckpointDir = tempDir.resolve("checkpoints", "out")
      val resultDir = tempDir.resolve("result")

      val config = yaml.load[TransformConfig](
        s"""
           |tasks:
           |- datasetID: out
           |  source:
           |    inputs:
           |      - id: in
           |    transform:
           |      engine: flink
           |      watermarks:
           |      - id: in
           |        eventTimeColumn: event_time
           |      query: >
           |        SELECT
           |          TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
           |          symbol as symbol,
           |          min(price) as `min`,
           |          max(price) as `max`
           |        FROM `in`
           |        GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
           |  inputSlices:
           |    in:
           |      hash: ""
           |      interval: "(-inf, inf)"
           |      numRecords: 0
           |  datasetLayouts:
           |    in:
           |      metadataDir: /none
           |      dataDir: $inputDataDir
           |      checkpointsDir: /none
           |      cacheDir: /none
           |    out:
           |      metadataDir: /none
           |      dataDir: $outputDataDir
           |      checkpointsDir: $outputCheckpointDir
           |      cacheDir: /none
           |  datasetVocabs:
           |    in:
           |      systemTimeColumn: system_time
           |      corruptRecordColumn: __corrupt_record__
           |    out:
           |      systemTimeColumn: system_time
           |      corruptRecordColumn: __corrupt_record__
           |  resultDir: $resultDir
           |""".stripMargin
      )

      {
        writeParquet(
          tempDir.resolve(inputDataDir).resolve("1.parquet"),
          Seq(
            Ticker(ts(1, 1), "A", 10),
            Ticker(ts(1, 1), "B", 20),
            Ticker(ts(1, 2), "A", 11),
            Ticker(ts(1, 2), "B", 21),
            Ticker(ts(2, 1), "A", 12),
            Ticker(ts(2, 1), "B", 22),
            Ticker(ts(2, 2), "A", 13),
            Ticker(ts(2, 2), "B", 23),
            Ticker(ts(3, 1), "A", 14),
            Ticker(ts(3, 1), "B", 24),
            Ticker(ts(3, 2), "A", 15),
            Ticker(ts(3, 2), "B", 25)
          )
        )

        val result = engineRunner.run(config.tasks(0))

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(1), "A", 10, 11),
          TickerSummary(ts(1), "B", 20, 21),
          TickerSummary(ts(2), "A", 12, 13),
          TickerSummary(ts(2), "B", 22, 23)
        )
      }

      {
        writeParquet(
          tempDir.resolve(inputDataDir).resolve("2.parquet"),
          Seq(
            Ticker(ts(4, 1), "A", 16),
            Ticker(ts(4, 1), "B", 26),
            Ticker(ts(4, 2), "A", 17),
            Ticker(ts(4, 2), "B", 27),
            Ticker(ts(5, 1), "A", 18),
            Ticker(ts(5, 1), "B", 28),
            Ticker(ts(5, 2), "A", 19),
            Ticker(ts(5, 2), "B", 29)
          )
        )

        val result = engineRunner.run(config.tasks(0))

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(3), "A", 14, 15),
          TickerSummary(ts(3), "B", 24, 25),
          TickerSummary(ts(4), "A", 16, 17),
          TickerSummary(ts(4), "B", 26, 27)
        )
      }

      {
        writeParquet(
          tempDir.resolve(inputDataDir).resolve("3.parquet"),
          Seq(
            Ticker(ts(6, 1), "A", 20),
            Ticker(ts(6, 1), "B", 30)
          )
        )

        val result = engineRunner.run(config.tasks(0))

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(5), "A", 18, 19),
          TickerSummary(ts(5), "B", 28, 29)
        )
      }
    }
  }

  ignore("Tumbling window aggregation with watermark") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val engineRunner =
        new EngineRunner(fileSystem, new DockerClient(fileSystem))

      val inputDataDir = tempDir.resolve("data", "in")
      val outputDataDir = tempDir.resolve("data", "out")
      val outputCheckpointDir = tempDir.resolve("checkpoints", "out")
      val resultDir = tempDir.resolve("result")

      val config = yaml.load[TransformConfig](
        s"""
          |tasks:
          |- datasetID: out
          |  source:
          |    inputs:
          |      - id: in
          |    transform:
          |      engine: flink
          |      watermarks:
          |      - id: in
          |        eventTimeColumn: event_time
          |        maxLateBy: 1 day
          |      query: >
          |        SELECT
          |          TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
          |          symbol as symbol,
          |          min(price) as `min`,
          |          max(price) as `max`
          |        FROM `in`
          |        GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
          |  inputSlices:
          |    in:
          |      hash: ""
          |      interval: "(-inf, inf)"
          |      numRecords: 0
          |  datasetLayouts:
          |    in:
          |      metadataDir: /none
          |      dataDir: $inputDataDir
          |      checkpointsDir: /none
          |      cacheDir: /none
          |    out:
          |      metadataDir: /none
          |      dataDir: $outputDataDir
          |      checkpointsDir: $outputCheckpointDir
          |      cacheDir: /none
          |  datasetVocabs:
          |    in:
          |      systemTimeColumn: system_time
          |      corruptRecordColumn: __corrupt_record__
          |    out:
          |      systemTimeColumn: system_time
          |      corruptRecordColumn: __corrupt_record__
          |  resultDir: $resultDir
          |""".stripMargin
      )

      {
        writeParquet(
          tempDir.resolve(inputDataDir).resolve("1.parquet"),
          Seq(
            Ticker(ts(1, 1), "A", 10),
            Ticker(ts(1, 1), "B", 20),
            Ticker(ts(1, 2), "A", 10),
            Ticker(ts(1, 2), "B", 21),
            Ticker(ts(2, 1), "A", 12),
            Ticker(ts(2, 1), "B", 22),
            Ticker(ts(2, 2), "A", 13),
            Ticker(ts(2, 2), "B", 23),
            Ticker(ts(1, 3), "A", 11), // One day late and will be considered
            Ticker(ts(3, 1), "A", 14),
            Ticker(ts(3, 1), "B", 24),
            Ticker(ts(3, 2), "A", 15),
            Ticker(ts(3, 2), "B", 25)
          )
        )

        val result = engineRunner.run(config.tasks(0))

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(1), "A", 10, 11),
          TickerSummary(ts(1), "B", 20, 21)
        )
      }

      {
        writeParquet(
          tempDir.resolve(inputDataDir).resolve("2.parquet"),
          Seq(
            Ticker(ts(1, 4), "A", 12), // Two days late and will be discarded
            Ticker(ts(4, 1), "A", 16),
            Ticker(ts(4, 1), "B", 26),
            Ticker(ts(4, 2), "A", 17),
            Ticker(ts(4, 2), "B", 27),
            Ticker(ts(5, 1), "A", 18),
            Ticker(ts(5, 1), "B", 28)
          )
        )

        val result = engineRunner.run(config.tasks(0))

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(2), "A", 12, 13),
          TickerSummary(ts(2), "B", 22, 23),
          TickerSummary(ts(3), "A", 14, 15),
          TickerSummary(ts(3), "B", 24, 25)
        )
      }
    }
  }
}
