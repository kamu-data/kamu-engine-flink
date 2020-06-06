package dev.kamu.engine.flink.test

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import com.sksamuel.avro4s._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{TransformConfig, TransformResult}
import dev.kamu.core.utils.{
  DockerClient,
  DockerProcessBuilder,
  DockerRunArgs,
  ExecArgs,
  IOHandlerPresets,
  ManualClock
}
import dev.kamu.core.utils.fs._
import dev.kamu.engine.flink.{CustomLocalStreamExecutionEnvironment, Engine}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._

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

  def createEngine(): Engine = {
    val systemClock = new ManualClock()
    systemClock.advance()

    val env = new StreamExecutionEnvironment(
      new CustomLocalStreamExecutionEnvironment()
    )
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    new Engine(fileSystem, systemClock, env, tEnv)
  }

  def runEngine(
    config: TransformConfig,
    volumeMap: Map[Path, Path],
    resultPath: Path
  ): TransformResult = {
    val dockerClient = new DockerClient(fileSystem)

    val image = "kamudata/engine-flink:0.1.0"
    val networkName = "kamu-flink"

    val engineJar = new Path(".")
      .resolve("target", "scala-2.12", "engine.flink.jar")

    val configPathInContainer = new Path("/opt/engine/config.yaml")
    val engineJarInContainer = new Path("/opt/engine/bin/engine.flink.jar")

    Temp.withTempFile(
      fileSystem,
      "kamu-config-",
      os => yaml.save(Manifest(config), os)
    ) { tempConfigPath =>
      if (!fileSystem.exists(engineJar))
        throw new RuntimeException(s"Assembly does not exist: $engineJar")

      dockerClient.withNetwork(networkName) {

        val jobManager = new DockerProcessBuilder(
          "jobmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("jobmanager"),
            hostname = Some("jobmanager"),
            args = List("jobmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6123, 8081),
            network = Some(networkName),
            volumeMap = Map(
              engineJar -> engineJarInContainer,
              tempConfigPath -> configPathInContainer
            ) ++ volumeMap
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("jobmanager: ")))

        val taskManager = new DockerProcessBuilder(
          "taskmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("taskmanager"),
            hostname = Some("taskmanager"),
            args = List("taskmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6121, 6122),
            network = Some(networkName),
            volumeMap = volumeMap
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("taskmanager: ")))

        jobManager.waitForHostPort(8081, 15 seconds)

        val unix = new com.sun.security.auth.module.UnixSystem()

        val chownCmd = s"chown -R ${unix.getUid}:${unix.getGid} " + volumeMap.values
          .map(_.toString)
          .mkString(" ")

        val exitCode = dockerClient
          .exec(
            ExecArgs(),
            jobManager.containerName,
            Seq("bash", "-c", s"flink run $engineJarInContainer; $chownCmd")
          )
          .!

        if (exitCode != 0)
          throw new RuntimeException(
            s"Engine run failed with exit code: $exitCode"
          )

        taskManager.kill()
        jobManager.kill()

        taskManager.join()
        jobManager.join()
      }
    }

    val inputStream = fileSystem.open(resultPath)
    val result = yaml.load[Manifest[TransformResult]](inputStream).content
    inputStream.close()

    fileSystem.delete(resultPath, false)

    result
  }

  test("Tumbling window aggregation") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      val inputDataDir = new Path("data", "in")
      val outputDataDir = new Path("data", "out")
      val outputCheckpointDir = new Path("checkpoints", "out")
      val resultPath = new Path("result.yaml")

      val workspaceDir = new Path("/opt/workspace")

      val config = TransformConfig.load(
        s"""
          |apiVersion: 1
          |kind: TransformConfig
          |content:
          |  tasks:
          |  - datasetID: out
          |    source:
          |      inputs:
          |        - id: in
          |      transform:
          |        engine: flink
          |        watermarks:
          |        - id: in
          |          eventTimeColumn: event_time
          |          maxLateBy: 1 day
          |        query: >
          |          SELECT
          |            TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
          |            symbol as symbol,
          |            min(price) as `min`,
          |            max(price) as `max`
          |          FROM `in`
          |          GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
          |    inputSlices:
          |      in:
          |        hash: ""
          |        interval: "(-inf, inf)"
          |        numRecords: 0
          |    datasetLayouts:
          |      in:
          |        metadataDir: /none
          |        dataDir: ${workspaceDir.resolve(inputDataDir)}
          |        checkpointsDir: /none
          |        cacheDir: /none
          |      out:
          |        metadataDir: /none
          |        dataDir: ${workspaceDir.resolve(outputDataDir)}
          |        checkpointsDir: ${workspaceDir.resolve(outputCheckpointDir)}
          |        cacheDir: /none
          |    datasetVocabs:
          |      in:
          |        systemTimeColumn: system_time
          |        corruptRecordColumn: __corrupt_record__
          |      out:
          |        systemTimeColumn: system_time
          |        corruptRecordColumn: __corrupt_record__
          |    resultPath: ${workspaceDir.resolve(resultPath)}
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

        val result = runEngine(
          config,
          Map(tempDir -> workspaceDir),
          tempDir.resolve(resultPath)
        )

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

        val result = runEngine(
          config,
          Map(tempDir -> workspaceDir),
          tempDir.resolve(resultPath)
        )

        println(result.block)

        val actual = readParquet[TickerSummary](
          tempDir.resolve(outputDataDir).resolve(result.dataFileName.get)
        ).sortBy(i => (i.event_time.getTime, i.symbol))

        actual shouldEqual List(
          TickerSummary(ts(2), "A", 12, 13),
          TickerSummary(ts(2), "B", 22, 23),
          TickerSummary(ts(3), "A", 14, 15),
          TickerSummary(ts(3), "B", 15, 16)
        )
      }
    }
  }
}
