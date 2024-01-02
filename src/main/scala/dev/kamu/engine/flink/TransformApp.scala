package dev.kamu.engine.flink

import java.nio.file.Paths
import better.files.File
import dev.kamu.core.manifests.{TransformRequest, TransformResponse}
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, StringWriter}
import java.time.ZoneId

object TransformApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val responsePath = Paths.get("/opt/engine/in-out/response.yaml")

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    if (!File(requestPath).exists)
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[TransformRequest](requestPath)

    def saveResponse(response: TransformResponse): Unit = {
      yaml.save(response, responsePath)
    }

    logger.info(s"Executing request: $request")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setParallelism(1)

    // We don't want checkpointing or restart attempts
    // See: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/task_failure_recovery/
    env.setRestartStrategy(RestartStrategies.noRestart())
    assert(!env.getCheckpointConfig.isCheckpointingEnabled)

    // Checkpointing is disabled, but same mechanism is used for savepoints, so we configure it to tolerate large state
    // See: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/checkpoints/
    env.getCheckpointConfig
      .setCheckpointStorage(
        new FileSystemCheckpointStorage(
          "file:///opt/engine/checkpoints/"
        )
      )

    // See: https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/execution/execution_configuration/
    env.getConfig.enableObjectReuse()

    // See: https://flink.apache.org/news/2020/04/15/flink-serialization-tuning-vol-1.html#row-data-types
    env.getConfig.disableGenericTypes()

    tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

    logger.info(
      s"Processing dataset: ${request.datasetAlias} (${request.datasetId})"
    )

    val engine = new TransformEngine(env, tEnv)

    try {
      val response = engine.executeTransform(request)
      logger.info(s"Processing result: ${request.datasetAlias}\n$response")
      saveResponse(response)
    } catch {
      case e: org.apache.flink.table.api.ValidationException =>
        saveResponse(TransformResponse.InvalidQuery(e.toString))
        throw e
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        saveResponse(
          TransformResponse.InternalError(e.toString, Some(sw.toString))
        )
        throw e
    }

    logger.info(
      s"Done processing dataset: ${request.datasetAlias} (${request.datasetId})"
    )

    logger.info("Finished")
  }
}
