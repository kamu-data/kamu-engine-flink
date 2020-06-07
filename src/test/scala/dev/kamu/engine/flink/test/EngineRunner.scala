package dev.kamu.engine.flink.test

import scala.concurrent.duration._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{
  TransformConfig,
  TransformResult,
  TransformTaskConfig
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.fs.Temp
import dev.kamu.core.utils.{
  DockerClient,
  DockerProcessBuilder,
  DockerRunArgs,
  ExecArgs,
  IOHandlerPresets
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

class EngineRunner(
  fileSystem: FileSystem,
  dockerClient: DockerClient,
  image: String = "kamudata/engine-flink:0.1.0",
  networkName: String = "kamu-flink"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    task: TransformTaskConfig
  ): TransformResult = {
    val engineJar = new Path(".")
      .resolve("target", "scala-2.12", "engine.flink.jar")

    val configPathInContainer = new Path("/opt/engine/config.yaml")
    val engineJarInContainer = new Path("/opt/engine/bin/engine.flink.jar")

    val volumeMap = toVolumeMap(task)

    Temp.withTempFile(
      fileSystem,
      "kamu-config-",
      os => yaml.save(Manifest(TransformConfig(Vector(task))), os)
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

        val prevSavepoint = getPrevSavepoint(task)
        val savepointArgs = prevSavepoint.map(p => s"-s $p").getOrElse("")

        val unix = new com.sun.security.auth.module.UnixSystem()
        val chownCmd = s"chown -R ${unix.getUid}:${unix.getGid} " + volumeMap.values
          .map(_.toString)
          .mkString(" ")

        val exitCode = dockerClient
          .exec(
            ExecArgs(),
            jobManager.containerName,
            Seq(
              "bash",
              "-c",
              s"flink run $savepointArgs $engineJarInContainer; $chownCmd"
            )
          )
          .!

        if (exitCode != 0)
          throw new RuntimeException(
            s"Engine run failed with exit code: $exitCode"
          )

        commitSavepoint(prevSavepoint)

        taskManager.kill()
        jobManager.kill()

        taskManager.join()
        jobManager.join()
      }
    }

    val resultPath = task.resultDir.resolve("result.yaml")
    val inputStream = fileSystem.open(resultPath)
    val result = yaml.load[Manifest[TransformResult]](inputStream).content
    inputStream.close()

    fileSystem.delete(resultPath, false)

    result
  }

  protected def getPrevSavepoint(task: TransformTaskConfig): Option[Path] = {
    val checkpointsDir =
      task.datasetLayouts(task.datasetID.toString).checkpointsDir

    if (!fileSystem.exists(checkpointsDir))
      return None

    val allSavepoints = fileSystem.listStatus(checkpointsDir).map(_.getPath)

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

    oldSavepoint.foreach(fileSystem.delete(_, true))
  }

  protected def toVolumeMap(task: TransformTaskConfig): Map[Path, Path] = {
    val layoutPaths = task.datasetLayouts.values
      .flatMap(l => List(l.checkpointsDir, l.dataDir))

    (layoutPaths ++ List(task.resultDir)).map(p => (p, p)).toMap
  }
}
