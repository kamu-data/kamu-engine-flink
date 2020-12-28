package dev.kamu.engine.flink

import java.nio.file.Paths
import java.time.Instant
import better.files.File
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import pureconfig.generic.auto._
import dev.kamu.core.utils.ManualClock
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.Manifest
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.slf4j.LoggerFactory

object EngineApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val resultPath = Paths.get("/opt/engine/in-out/result.yaml")

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    if (!File(requestPath).exists)
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[Manifest[ExecuteQueryRequest]](requestPath).content

    logger.info(s"Executing request: $request")

    val systemClock = new ManualClock()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // See: https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/execution/execution_configuration/
    env.getConfig.enableObjectReuse()

    // See: https://flink.apache.org/news/2020/04/15/flink-serialization-tuning-vol-1.html#row-data-types
    env.getConfig.disableGenericTypes()

    val timeOverride = sys.env.get("KAMU_SYSTEM_TIME").map(Instant.parse)
    if (timeOverride.isEmpty)
      systemClock.advance()
    else
      systemClock.set(timeOverride.get)

    logger.info(s"Processing dataset: ${request.datasetID}")

    val engine = new Engine(systemClock, env, tEnv)
    val result = engine.executeQueryExtended(request)

    yaml.save(Manifest(result), resultPath)
    logger.info(s"Done processing dataset: ${request.datasetID}")

    logger.info("Finished")
  }
}
