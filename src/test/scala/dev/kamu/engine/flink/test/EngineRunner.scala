package dev.kamu.engine.flink.test

import java.nio.file.{Path, Paths}
import better.files.File

import scala.concurrent.duration._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.{
  RawQueryRequest,
  RawQueryResponse,
  SqlQueryStep,
  Transform,
  TransformRequest,
  TransformResponse
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.Temp
import dev.kamu.core.utils.{
  DockerClient,
  DockerProcessBuilder,
  DockerRunArgs,
  ExecArgs,
  IOHandlerPresets
}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

import scala.reflect.ClassTag

class EngineRunner(
  dockerClient: DockerClient,
  image: String =
    "ghcr.io/kamu-data/engine-flink:0.17.0-flink_1.16.0-scala_2.12-java8",
  networkName: String = "kamu-flink"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def executeRawQuery(
    request: RawQueryRequest,
    workspaceDir: Path
  ): RawQueryResponse.Success = {
    run[RawQueryRequest, RawQueryResponse](
      request,
      workspaceDir,
      "dev.kamu.engine.flink.RawQueryApp",
      ""
    ).asInstanceOf[RawQueryResponse.Success]
  }

  def executeTransform(
    requestRaw: TransformRequest,
    workspaceDir: Path
  ): TransformResponse.Success = {
    // Normalize request (this is normally done by coordinator)
    val request = {
      val transformRaw = requestRaw.transform.asInstanceOf[Transform.Sql]
      requestRaw.copy(
        transform = if (transformRaw.query.isDefined) {
          transformRaw.copy(
            queries = Some(Vector(SqlQueryStep(None, transformRaw.query.get)))
          )
        } else {
          transformRaw
        }
      )
    }

    // Prepare savepoint
    val newCheckpointPath = request.newCheckpointPath
    val prevSavepoint =
      request.prevCheckpointPath.map(p => getPrevSavepoint(p))
    val savepointArgs = prevSavepoint.map(p => s"-s $p").getOrElse("")

    try {
      run[TransformRequest, TransformResponse](
        request,
        workspaceDir,
        "dev.kamu.engine.flink.TransformApp",
        savepointArgs
      ).asInstanceOf[TransformResponse.Success]
    } catch {
      case e: RuntimeException =>
        if (newCheckpointPath.toFile.exists()) {
          FileUtils.deleteDirectory(newCheckpointPath.toFile)
        }
        throw e
    }
  }

  private def run[Req: ClassTag, Resp: ClassTag](
    request: Req,
    workspaceDir: Path,
    mainClass: String,
    extraArgs: String
  )(
    implicit writer: Derivation[ConfigWriter[Req]],
    reader: Derivation[ConfigReader[Resp]]
  ): Resp = {
    val engineJar = Paths.get("target", "scala-2.12", "engine.flink.jar")

    if (!File(engineJar).exists)
      throw new RuntimeException(s"Assembly does not exist: $engineJar")

    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.flink.jar")

    val volumeMap = Map(workspaceDir -> workspaceDir)

    Temp.withRandomTempDir("kamu-inout-") { inOutDir =>
      yaml.save[Req](request, inOutDir.resolve("request.yaml"))

      dockerClient.withNetwork(networkName) {

        val jobManager = new DockerProcessBuilder(
          "jobmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("jobmanager"),
            hostname = Some("jobmanager"),
            entryPoint = Some("/docker-entrypoint.sh"),
            args = List("jobmanager"),
            environmentVars = Map(
              "JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"
            ),
            exposePorts = List(6123, 8081),
            network = Some(networkName),
            volumeMap = Map(
              engineJar -> engineJarInContainer,
              inOutDir -> inOutDirInContainer
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
            entryPoint = Some("/docker-entrypoint.sh"),
            args = List("taskmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6121, 6122),
            network = Some(networkName),
            volumeMap = volumeMap
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("taskmanager: ")))

        jobManager.waitForHostPort(8081, 15 seconds)

        try {
          val exitCode = dockerClient
            .exec(
              ExecArgs(),
              jobManager.containerName,
              Seq(
                "bash",
                "-c",
                s"flink run -c $mainClass $extraArgs $engineJarInContainer"
              )
            )
            .!

          if (exitCode != 0) {
            throw new RuntimeException(
              s"Engine run failed with exit code: $exitCode"
            )
          }

        } finally {
          val unix = new com.sun.security.auth.module.UnixSystem()
          val chownCmd = s"chown -R ${unix.getUid}:${unix.getGid} " + volumeMap.values
            .map(_.toString)
            .mkString(" ")

          dockerClient
            .exec(
              ExecArgs(),
              jobManager.containerName,
              Seq("bash", "-c", chownCmd)
            )
            .!

          taskManager.kill()
          jobManager.kill()

          taskManager.join()
          jobManager.join()
        }
      }

      yaml
        .load[Resp](inOutDir.resolve("response.yaml"))
    }
  }

  protected def getPrevSavepoint(prevCheckpointDir: Path): Path = {
    val allSavepoints = prevCheckpointDir.toFile
      .listFiles()
      .filter(_.isDirectory)
      .filter(_.getName.startsWith("savepoint-"))
      .map(_.toPath)
      .toList

    // TODO: Atomicity
    if (allSavepoints.length > 1)
      throw new RuntimeException(
        "Multiple checkpoints found: " + allSavepoints.mkString(", ")
      )

    logger.info("Using savepoint: {}", allSavepoints.head)

    allSavepoints.head
  }
}
