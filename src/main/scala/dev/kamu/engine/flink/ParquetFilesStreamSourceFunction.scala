package dev.kamu.engine.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.connector.file.src.reader.BulkFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.{
  RichSourceFunction,
  SourceFunction
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.data.RowData
import org.apache.flink.types.RowKind
import org.slf4j.LoggerFactory

import java.net.URI
import java.time.Instant

class ParquetFilesStreamSourceFunction(
  sourceName: String,
  filesToRead: Vector[String],
  inputFormat: BulkFormat[RowData, FileSourceSplit],
  rowKindExtractor: RowData => RowKind,
  timestampExtractor: RowData => Long,
  prevWatermark: Option[Instant],
  explicitWatermarks: Vector[Instant],
  terminateWhenExhausted: Boolean,
  exhaustedMarkerPath: Option[String]
) extends RichSourceFunction[RowData]
    with ResultTypeQueryable[RowData]
    with Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  private var configuration: Configuration = _
  @volatile private var isCancelled = false

  override def open(config: Configuration): Unit = {
    super.open(config)
    this.configuration = config

  }

  override def run(ctx: SourceFunction.SourceContext[RowData]): Unit = {
    var currentWatermark = Long.MinValue

    if (prevWatermark.isDefined) {
      logger.info(
        "Source '{}': Emitting initial watermark: {}",
        sourceName: Any,
        prevWatermark: Any
      )
      currentWatermark = prevWatermark.get.toEpochMilli
      ctx.emitWatermark(new Watermark(currentWatermark))
    }

    var splitIndex = 0;

    for (path <- filesToRead) {
      logger.info(
        "Source '{}': opening input file: {}",
        sourceName: Any,
        path: Any
      )

      // TODO: reading as a single split which might not work for large data
      val reader =
        inputFormat.createReader(
          this.configuration,
          new FileSourceSplit(
            s"${sourceName}_${splitIndex}",
            new Path(URI.create(path)),
            0,
            Long.MaxValue,
            0,
            Long.MaxValue
          )
        )

      splitIndex += 1

      var batch = reader.readBatch()

      while (batch != null) {
        var record = batch.next()

        while (record != null) {
          val rowData = record.getRecord
          val rowKind = rowKindExtractor(rowData)
          val timestamp = timestampExtractor(rowData)

          // TODO: Support different watermarking strategies
          // Currently this source only emits watermarks at the beginning and the end of processing
          // We may want to have watermarks for intermediate events too and be able to define watermark lag.
          rowData.setRowKind(rowKind)
          ctx.collectWithTimestamp(rowData, timestamp)

          record = batch.next()
        }

        batch.releaseBatch()
        batch = reader.readBatch()
      } // batch

      logger.info(
        "Source '{}': Closing input file: {}",
        sourceName: Any,
        path: Any
      )

      reader.close()
    } // path

    for (instant <- explicitWatermarks) {
      val watermark = instant.toEpochMilli
      if (watermark > currentWatermark) {
        logger.info(
          "Source '{}': Emitting explicit end watermark: {}",
          sourceName: Any,
          instant: Any
        )
        ctx.emitWatermark(new Watermark(instant.toEpochMilli))
        currentWatermark = watermark
      }
    }

    if (terminateWhenExhausted) {
      logger.info(
        "Source '{}' read all input data, terminating",
        sourceName
      )
    } else {
      logger.info(
        "Source '{}' read all input data, creating marker file at: {}",
        sourceName,
        exhaustedMarkerPath.get: Any
      )

      new java.io.File(exhaustedMarkerPath.get).createNewFile()

      // TODO: Marking source idle makes CombinedWatermarkStatus ignore watermark on this source
      // which throws off all computations
      // ctx.markAsTemporarilyIdle()

      while (!isCancelled) {
        Thread.sleep(50)
      }
    }
  }

  override def cancel(): Unit = {
    logger.info("Source {}: Cancelled", sourceName: Any)
    isCancelled = true
  }

  override def getProducedType: TypeInformation[RowData] = {
    inputFormat.getProducedType
  }
}
