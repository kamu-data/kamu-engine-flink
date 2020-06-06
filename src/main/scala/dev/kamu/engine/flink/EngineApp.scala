package dev.kamu.engine.flink

import dev.kamu.core.manifests.infra.TransformConfig
import dev.kamu.core.utils.ManualClock
import dev.kamu.core.utils.fs._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

object EngineApp {
  val configPath = new Path("/opt/engine/config.yaml")

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(getClass.getName)

    val fileSystem = FileSystem.get(new Configuration())

    if (!fileSystem.exists(configPath))
      throw new RuntimeException(s"Could not find config: $configPath")

    val inputStream = fileSystem.open(configPath)
    val config = TransformConfig.load(inputStream)
    inputStream.close()

    /*val config = TransformConfig.load(
      """
        |apiVersion: 1
        |kind: TransformConfig
        |content:
        |  tasks:
        |  - datasetID: b
        |    source:
        |      inputs:
        |        - id: a
        |      transform:
        |        engine: flink
        |        watermarks:
        |        - id: a
        |          eventTimeColumn: event_time
        |          maxLateBy: 1 day
        |        query: >
        |          SELECT
        |            TUMBLE_START(event_time, INTERVAL '1' DAY) as start_time,
        |            TUMBLE_END(event_time, INTERVAL '1' DAY) as end_time,
        |            count(*) as `num_transactions`,
        |            - min(delta) as `largest_expense`,
        |            - sum(delta) as `total_spent`
        |          FROM a
        |          WHERE delta < 0
        |          GROUP BY TUMBLE(event_time, INTERVAL '1' DAY)
        |    inputSlices:
        |      a:
        |        hash: ""
        |        interval: "(-inf, inf)"
        |        numRecords: 0
        |    datasetLayouts:
        |      a:
        |        metadataDir: workspace/meta/a
        |        dataDir: workspace/data/a
        |        checkpointsDir: workspace/checkpoints/a
        |        cacheDir: workspace/cache/a
        |      b:
        |        metadataDir: workspace/meta/b
        |        dataDir: workspace/data/b
        |        checkpointsDir: workspace/checkpoints/b
        |        cacheDir: workspace/cache/b
        |    datasetVocabs:
        |      a:
        |        systemTimeColumn: system_time
        |        corruptRecordColumn: __corrupt_record__
        |      b:
        |        systemTimeColumn: system_time
        |        corruptRecordColumn: __corrupt_record__
        |    metadataOutputDir: workspace/
        |""".stripMargin
    )*/

    /*
    val rootDir = new Path("/opt/engine")
    val inputDir = rootDir.resolve("workspace/data/tickers")
    val outputDataDir = rootDir.resolve("workspace/data/tickers_summary")
    val outputCheckpointDir =
      rootDir.resolve("workspace/checkpoints/tickers_summary")
    val outputDir = rootDir.resolve("workspace")
    val config = TransformConfig.load(
      s"""
         |apiVersion: 1
         |kind: TransformConfig
         |content:
         |  tasks:
         |  - datasetID: result
         |    source:
         |      inputs:
         |        - id: tickers
         |      transform:
         |        engine: flink
         |        watermarks:
         |        - id: tickers
         |          eventTimeColumn: event_time
         |          maxLateBy: 1 day
         |        query: >
         |          SELECT
         |            TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
         |            symbol as symbol,
         |            min(price) as `min`,
         |            max(price) as `max`
         |          FROM tickers
         |          GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
         |    inputSlices:
         |      tickers:
         |        hash: ""
         |        interval: "(-inf, inf)"
         |        numRecords: 0
         |    datasetLayouts:
         |      tickers:
         |        metadataDir: /none
         |        dataDir: $inputDir
         |        checkpointsDir: /none
         |        cacheDir: /none
         |      result:
         |        metadataDir: /none
         |        dataDir: $outputDataDir
         |        checkpointsDir: $outputCheckpointDir
         |        cacheDir: /none
         |    datasetVocabs:
         |      tickers:
         |        systemTimeColumn: system_time
         |        corruptRecordColumn: __corrupt_record__
         |      result:
         |        systemTimeColumn: system_time
         |        corruptRecordColumn: __corrupt_record__
         |    metadataOutputDir: $outputDir
         |""".stripMargin
    )
     */

    if (config.tasks.isEmpty) {
      logger.warn("No tasks specified")
      return
    }

    logger.info(s"Running with config: $config")

    val systemClock = new ManualClock()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val env = new StreamExecutionEnvironment(new CustomLocalStreamExecutionEnvironment())
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //env.getJavaEnv.getConfig.disableAutoGeneratedUIDs()

    for (taskConfig <- config.tasks) {
      systemClock.advance()
      logger.info(s"Processing dataset: ${taskConfig.datasetID}")

      val engine = new Engine(fileSystem, systemClock, env, tEnv)
      engine.executeQueryExtended(taskConfig)

      logger.info(s"Done processing dataset: ${taskConfig.datasetID}")
    }

    logger.info("Finished")
  }
}
