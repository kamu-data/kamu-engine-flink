package dev.kamu.engine.flink.test

import java.nio.file.Path
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.{ExecuteQueryRequest, ExecuteQueryInput}
import dev.kamu.core.utils.fs._

import scala.util.Random

trait HasOffset {
  def getOffset: Long
}

trait EngineHelpers {

  def randomDataFileName(): String = {
    Random.alphanumeric.take(10).mkString("") + ".parquet"
  }

  def withRandomOutputPath(
    request: ExecuteQueryRequest,
    layout: DatasetLayout,
    prevCheckpointPath: Option[Path] = None
  ): ExecuteQueryRequest = {
    request.copy(
      outDataPath = layout.dataDir.resolve(randomDataFileName()),
      prevCheckpointPath = prevCheckpointPath,
      newCheckpointPath = layout.checkpointsDir
        .resolve(Random.alphanumeric.take(10).mkString(""))
    )
  }

  def withInputData[T <: HasOffset: Encoder: Decoder](
    request: ExecuteQueryRequest,
    datasetName: String,
    dataDir: Path,
    data: Seq[T]
  )(
    implicit schemaFor: SchemaFor[T]
  ): ExecuteQueryRequest = {
    val dataInterval = OffsetInterval(
      start = data.map(_.getOffset).min,
      end = data.map(_.getOffset).max
    )
    val inputPath = dataDir.resolve(randomDataFileName())

    ParquetHelpers.write(
      inputPath,
      data
    )

    request.inputs.indexWhere(_.datasetName.toString == datasetName) match {
      case -1 =>
        request.copy(
          inputs = request.inputs ++ Vector(
            ExecuteQueryInput(
              datasetID = DatasetID("did:odf:" + datasetName),
              datasetName = DatasetName(datasetName),
              dataInterval = Some(dataInterval),
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
          input.copy(
            dataPaths = input.dataPaths ++ Vector(inputPath),
            dataInterval =
              Some(input.dataInterval.get.copy(end = dataInterval.end))
          )
        request.copy(inputs = request.inputs.updated(i, newInput))
    }
  }

}
