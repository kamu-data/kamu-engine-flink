package dev.kamu.engine.flink.test

import java.nio.file.Path

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra.{ExecuteQueryRequest, InputDataSlice}
import dev.kamu.core.utils.fs._
import spire.math.Interval

import scala.util.Random

trait EngineHelpers {

  def randomDataFileName(): String = {
    Random.alphanumeric.take(10).mkString("") + ".parquet"
  }

  def withRandomOutputPath(
    request: ExecuteQueryRequest,
    layout: DatasetLayout,
    prevCheckpointDir: Option[String] = None
  ): ExecuteQueryRequest = {
    request.copy(
      outDataPath = layout.dataDir.resolve(randomDataFileName()).toString,
      prevCheckpointDir = prevCheckpointDir,
      newCheckpointDir = layout.checkpointsDir
        .resolve(Random.alphanumeric.take(10).mkString(""))
        .toString
    )
  }

  def withInputData[T: Encoder: Decoder](
    request: ExecuteQueryRequest,
    datasetID: String,
    dataDir: Path,
    data: Seq[T]
  )(
    implicit schemaFor: SchemaFor[T]
  ): ExecuteQueryRequest = {
    val inputPath = dataDir.resolve(randomDataFileName())

    ParquetHelpers.write(
      inputPath,
      data
    )

    val otherDataPaths = request.inputSlices
      .get(datasetID)
      .map(_.dataPaths)
      .getOrElse(Vector.empty)

    request.copy(
      inputSlices = request.inputSlices ++ Map(
        datasetID -> InputDataSlice(
          interval = Interval.all,
          schemaFile = inputPath.toString,
          dataPaths = otherDataPaths ++ Vector(inputPath.toString),
          explicitWatermarks = Vector.empty
        )
      )
    )
  }

}
