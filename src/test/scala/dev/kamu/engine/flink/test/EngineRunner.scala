package dev.kamu.engine.flink.test

import java.nio.file.{Path, Paths}
import java.sql.Timestamp

import better.files.File

import scala.concurrent.duration._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{ExecuteQueryRequest, ExecuteQueryResult}
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
import org.slf4j.LoggerFactory

class EngineRunner(
  dockerClient: DockerClient,
  image: String = "kamudata/engine-flink:0.3.2",
  networkName: String = "kamu-flink"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    request: ExecuteQueryRequest,
    workspaceDir: Path,
    systemTime: Timestamp
  ): ExecuteQueryResult = {
    val engineJar = Paths.get("target", "scala-2.12", "engine.flink.jar")

    if (!File(engineJar).exists)
      throw new RuntimeException(s"Assembly does not exist: $engineJar")

    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.flink.jar")

    val volumeMap = Map(workspaceDir -> workspaceDir)

    Temp.withRandomTempDir("kamu-inout-") { inOutDir =>
      yaml.save(Manifest(request), inOutDir.resolve("request.yaml"))

      dockerClient.withNetwork(networkName) {

        val jobManager = new DockerProcessBuilder(
          "jobmanager",
          dockerClient,
          DockerRunArgs(
            image = image,
            containerName = Some("jobmanager"),
            hostname = Some("jobmanager"),
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
            args = List("taskmanager"),
            environmentVars = Map("JOB_MANAGER_RPC_ADDRESS" -> "jobmanager"),
            exposePorts = List(6121, 6122),
            network = Some(networkName),
            volumeMap = volumeMap
          )
        ).run(Some(IOHandlerPresets.redirectOutputTagged("taskmanager: ")))

        jobManager.waitForHostPort(8081, 15 seconds)

        val prevSavepoint = getPrevSavepoint(request)
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

          if (exitCode != 0)
            throw new RuntimeException(
              s"Engine run failed with exit code: $exitCode"
            )

          commitSavepoint(prevSavepoint)

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
        .load[Manifest[ExecuteQueryResult]](inOutDir.resolve("result.yaml"))
        .content
    }
  }

  protected def getPrevSavepoint(request: ExecuteQueryRequest): Option[Path] = {
    val checkpointsDir = File(request.checkpointsDir)

    if (!checkpointsDir.exists)
      return None

    val allSavepoints = checkpointsDir.list
      .filter(_.isDirectory)
      .map(_.path)
      .toList

    // TODO: Atomicity
    if (allSavepoints.length > 1)
      throw new RuntimeException(
        "Multiple checkpoints found: " + allSavepoints.mkString(", ")
      )

    logger.info("Using savepoint: {}", allSavepoints.headOption)

    allSavepoints.headOption
  }

  // TODO: Atomicity
  protected def commitSavepoint(oldSavepoint: Option[Path]): Unit = {
    if (oldSavepoint.isEmpty)
      return

    logger.info("Deleting savepoint: {}", oldSavepoint)

    oldSavepoint.foreach(p => File(p).delete(true))
  }
}
