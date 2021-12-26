package dev.kamu.engine.flink.test

import java.nio.file.Path
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dev.kamu.core.manifests.DatasetLayout
import dev.kamu.core.manifests.{ExecuteQueryRequest, Watermark}
import dev.kamu.core.utils.fs._

trait TimeHelpers {

  def ts(d: Int, h: Int = 0, m: Int = 0): Timestamp = {
    val dt = LocalDateTime.of(2000, 1, d, h, m)
    val zdt = ZonedDateTime.of(dt, ZoneOffset.UTC)
    Timestamp.from(zdt.toInstant)
  }

  def withWatermarks(
    request: ExecuteQueryRequest,
    wms: Map[String, Timestamp]
  ): ExecuteQueryRequest = {
    val wmsVec =
      wms.mapValues(
        eventTime => Vector(Watermark(ts(1).toInstant, eventTime.toInstant))
      )

    request.copy(
      inputs = request.inputs.map(
        i =>
          i.copy(
            explicitWatermarks =
              wmsVec.getOrElse(i.datasetName.toString, Vector.empty)
          )
      )
    )
  }

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName),
      cacheDir = workspaceDir.resolve("cache", datasetName)
    )
  }

}
