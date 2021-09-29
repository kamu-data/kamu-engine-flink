package dev.kamu.engine.flink.test

import java.nio.file.Path
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.{ExecuteQueryRequest, QueryInput}
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
    prevCheckpointDir: Option[Path] = None
  ): ExecuteQueryRequest = {
    request.copy(
      outDataPath = layout.dataDir.resolve(randomDataFileName()),
      prevCheckpointDir = prevCheckpointDir,
      newCheckpointDir = layout.checkpointsDir
        .resolve(Random.alphanumeric.take(10).mkString(""))
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

    request.inputs.indexWhere(_.datasetID.toString == datasetID) match {
      case -1 =>
        request.copy(
          inputs = request.inputs ++ Vector(
            QueryInput(
              datasetID = DatasetID(datasetID),
              interval = Interval.all,
              schemaFile = inputPath,
              dataPaths = Vector(inputPath),
              explicitWatermarks = Vector.empty,
              vocab = DatasetVocabulary(None, None)
            )
          )
        )
      case i =>
        val input = request.inputs(i)
        val newInput =
          input.copy(dataPaths = input.dataPaths ++ Vector(inputPath))
        request.copy(inputs = request.inputs.updated(i, newInput))
    }
  }

}
