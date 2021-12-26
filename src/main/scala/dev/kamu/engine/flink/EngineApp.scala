package dev.kamu.engine.flink

import java.nio.file.Paths
import better.files.File
import dev.kamu.core.manifests.{ExecuteQueryRequest, ExecuteQueryResponse}
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, StringWriter}

object EngineApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val responsePath = Paths.get("/opt/engine/in-out/response.yaml")

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    if (!File(requestPath).exists)
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[ExecuteQueryRequest](requestPath)
    def saveResponse(response: ExecuteQueryResponse): Unit = {
      yaml.save(response, responsePath)
    }

    logger.info(s"Executing request: $request")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // See: https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/execution/execution_configuration/
    env.getConfig.enableObjectReuse()

    // See: https://flink.apache.org/news/2020/04/15/flink-serialization-tuning-vol-1.html#row-data-types
    env.getConfig.disableGenericTypes()

    logger.info(
      s"Processing dataset: ${request.datasetName} (${request.datasetID})"
    )

    val engine = new Engine(env, tEnv)

    try {
      val response = engine.executeQueryExtended(request)
      saveResponse(response)
    } catch {
      case e: org.apache.flink.table.api.ValidationException =>
        saveResponse(ExecuteQueryResponse.InvalidQuery(e.toString))
        throw e
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        saveResponse(
          ExecuteQueryResponse.InternalError(e.toString, Some(sw.toString))
        )
        throw e
    }

    logger.info(
      s"Done processing dataset: ${request.datasetName} (${request.datasetID})"
    )

    logger.info("Finished")
  }
}
