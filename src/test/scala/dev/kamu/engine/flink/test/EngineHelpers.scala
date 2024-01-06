package dev.kamu.engine.flink.test

import java.nio.file.Path
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.{TransformRequest, TransformRequestInput}
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
    request: TransformRequest,
    layout: DatasetLayout,
    prevCheckpointPath: Option[Path] = None
  ): TransformRequest = {
    request.copy(
      newDataPath = layout.dataDir.resolve(randomDataFileName()),
      prevCheckpointPath = prevCheckpointPath,
      newCheckpointPath = layout.checkpointsDir
        .resolve(Random.alphanumeric.take(10).mkString(""))
    )
  }

  def withInputData[T <: HasOffset: Encoder: Decoder](
    request: TransformRequest,
    queryAlias: String,
    dataDir: Path,
    data: Seq[T],
    vocab: DatasetVocabulary = DatasetVocabulary.default()
  )(
    implicit schemaFor: SchemaFor[T]
  ): TransformRequest = {
    val offsetInterval = OffsetInterval(
      start = data.map(_.getOffset).min,
      end = data.map(_.getOffset).max
    )
    val inputPath = dataDir.resolve(randomDataFileName())

    ParquetHelpers.write(
      inputPath,
      data
    )

    request.queryInputs.indexWhere(_.queryAlias == queryAlias) match {
      case -1 =>
        request.copy(
          queryInputs = request.queryInputs ++ Vector(
            TransformRequestInput(
              datasetId = DatasetId("did:odf:" + queryAlias),
              datasetAlias = DatasetAlias(queryAlias),
              queryAlias = queryAlias,
              offsetInterval = Some(offsetInterval),
              schemaFile = inputPath,
              dataPaths = Vector(inputPath),
              explicitWatermarks = Vector.empty,
              vocab = vocab
            )
          )
        )
      case i =>
        val input = request.queryInputs(i)
        val newInput =
          input.copy(
            dataPaths = input.dataPaths ++ Vector(inputPath),
            offsetInterval =
              Some(input.offsetInterval.get.copy(end = offsetInterval.end))
          )
        request.copy(queryInputs = request.queryInputs.updated(i, newInput))
    }
  }

}
