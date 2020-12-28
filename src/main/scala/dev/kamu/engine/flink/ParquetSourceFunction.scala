package dev.kamu.engine.flink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.streaming.api.functions.source.{
  RichSourceFunction,
  SourceFunction
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI
import java.time.Instant

class ParquetSourceFunction(
  sourceName: String,
  filesToRead: Vector[String],
  inputFormat: ParquetRowInputFormatEx,
  prevWatermark: Instant,
  explicitWatermarks: Vector[Instant],
  markerPath: String
) extends RichSourceFunction[Row]
    with ResultTypeQueryable[Row]
    with Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  private var isCancelled = false

  override def setRuntimeContext(t: RuntimeContext): Unit = {
    super.setRuntimeContext(t)
    inputFormat.setRuntimeContext(t)
  }

  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    if (prevWatermark != Instant.MIN) {
      logger.info(
        "Source {}: Emitting initial watermark: {}",
        sourceName: Any,
        prevWatermark: Any
      )
      ctx.emitWatermark(new Watermark(prevWatermark.toEpochMilli))
    }

    for (path <- filesToRead) {
      logger.info(
        "Source {}: opening input file: {}",
        sourceName: Any,
        path: Any
      )

      inputFormat.open(
        new FileInputSplit(
          0,
          new org.apache.flink.core.fs.Path(URI.create(path)),
          0,
          0,
          null
        )
      )

      while (!inputFormat.reachedEnd()) {
        val row = inputFormat.nextRecord(null)
        ctx.collect(row)
      }

      logger.info(
        "Source {}: Closing input file: {}",
        sourceName: Any,
        path: Any
      )
      inputFormat.close()
    }

    for (instant <- explicitWatermarks) {
      logger.info(
        "Source {}: Emitting explicit end watermark: {}",
        sourceName: Any,
        instant: Any
      )
      ctx.emitWatermark(new Watermark(instant.toEpochMilli))
    }

    logger.info(
      "Source {}: Read all input files, creating marker file at: {}",
      sourceName: Any,
      markerPath: Any
    )
    new File(markerPath).createNewFile()

    ctx.markAsTemporarilyIdle()
    while (!isCancelled) {
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = {
    logger.info("Source {}: Cancelled", sourceName: Any)
    isCancelled = true
  }

  override def getProducedType(): TypeInformation[Row] = {
    inputFormat.getProducedType
  }
}
