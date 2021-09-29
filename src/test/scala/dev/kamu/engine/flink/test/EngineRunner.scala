package dev.kamu.engine.flink.test

import java.nio.file.{Path, Paths}
import java.sql.Timestamp

import better.files.File

import scala.concurrent.duration._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.{ExecuteQueryRequest, ExecuteQueryResponse}
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

class EngineRunner(
  dockerClient: DockerClient,
  image: String = "kamudata/engine-flink:0.10.0-flink_1.13.1-scala_2.12-java8",
  networkName: String = "kamu-flink"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    request: ExecuteQueryRequest,
    workspaceDir: Path,
    systemTime: Timestamp
  ): ExecuteQueryResponse.Success = {
    val engineJar = Paths.get("target", "scala-2.12", "engine.flink.jar")

    if (!File(engineJar).exists)
      throw new RuntimeException(s"Assembly does not exist: $engineJar")

    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.flink.jar")

    val volumeMap = Map(workspaceDir -> workspaceDir)

    Temp.withRandomTempDir("kamu-inout-") { inOutDir =>
      yaml.save(request, inOutDir.resolve("request.yaml"))

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
              "JOB_MANAGER_RPC_ADDRESS" -> "jobmanager",
              "KAMU_SYSTEM_TIME" -> systemTime.toInstant.toString
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

        val newCheckpointDir = request.newCheckpointDir
        val prevSavepoint =
          request.prevCheckpointDir.map(p => getPrevSavepoint(p))
        val savepointArgs = prevSavepoint.map(p => s"-s $p").getOrElse("")

        try {
          val exitCode = dockerClient
            .exec(
              ExecArgs(),
              jobManager.containerName,
              Seq(
                "bash",
                "-c",
                s"flink run $savepointArgs $engineJarInContainer"
              )
            )
            .!

          if (exitCode != 0) {
            if (newCheckpointDir.toFile.exists()) {
              FileUtils.deleteDirectory(newCheckpointDir.toFile)
            }
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
        .load[ExecuteQueryResponse](inOutDir.resolve("response.yaml"))
        .asInstanceOf[ExecuteQueryResponse.Success]
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
