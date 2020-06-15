package dev.kamu.engine.flink

import java.time.Instant

import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import pureconfig.generic.auto._
import dev.kamu.core.utils.ManualClock
import dev.kamu.core.utils.fs._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.Manifest
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

object EngineApp {
  val requestPath = new Path("/opt/engine/in-out/request.yaml")
  val resultPath = new Path("/opt/engine/in-out/result.yaml")

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(getClass.getName)

    val fileSystem = FileSystem.get(new Configuration())

    if (!fileSystem.exists(requestPath))
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request =
      yaml.load[Manifest[ExecuteQueryRequest]](fileSystem, requestPath).content

    logger.info(s"Executing request: $request")

    val systemClock = new ManualClock()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val timeOverride = sys.env.get("KAMU_SYSTEM_TIME").map(Instant.parse)
    if (timeOverride.isEmpty)
      systemClock.advance()
    else
      systemClock.set(timeOverride.get)

    logger.info(s"Processing dataset: ${request.datasetID}")

    val engine = new Engine(fileSystem, systemClock, env, tEnv)
    val result = engine.executeQueryExtended(request)

    yaml.save(Manifest(result), fileSystem, resultPath)
    logger.info(s"Done processing dataset: ${request.datasetID}")

    logger.info("Finished")
  }
}
