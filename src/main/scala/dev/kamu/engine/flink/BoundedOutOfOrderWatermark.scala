package dev.kamu.engine.flink

import org.apache.flink.api.common.eventtime.{
  TimestampAssigner,
  TimestampAssignerSupplier,
  WatermarkGenerator,
  WatermarkGeneratorSupplier,
  WatermarkOutput,
  WatermarkStrategy
}
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.duration.Duration

class MaxOutOfOrderWatermarkGenerator[T](
  maxOutOfOrderBy: Long
) extends WatermarkGenerator[T] {
  private val logger = LoggerFactory.getLogger(getClass)
  private var maxWatermark = Long.MinValue

  override def onEvent(
    event: T,
    eventTimestamp: Long,
    output: WatermarkOutput
  ): Unit = {
    val wm = eventTimestamp - maxOutOfOrderBy

    if (wm > maxWatermark) {
      logger.debug(s"Emitting watermark: ${Instant.ofEpochMilli(wm)}")
      maxWatermark = wm
      output.emitWatermark(
        new org.apache.flink.api.common.eventtime.Watermark(
          wm
        )
      )
    } else {
      logger.debug(s"Ignoring watermark: ${Instant.ofEpochMilli(wm)}")
    }
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {}
}

class ExtractorTimestampAssigner[T](extractor: T => Long)
    extends TimestampAssigner[T] {
  override def extractTimestamp(element: T, recordTimestamp: Long): Long = {
    extractor(element)
  }
}

class MaxOutOfOrderWatermarkStrategy[T](
  extractor: T => Long,
  maxOutOfOrderBy: Duration
) extends WatermarkStrategy[T] {
  override def createWatermarkGenerator(
    context: WatermarkGeneratorSupplier.Context
  ): WatermarkGenerator[T] = {
    new MaxOutOfOrderWatermarkGenerator[T](
      maxOutOfOrderBy.toMillis
    )
  }

  override def createTimestampAssigner(
    context: TimestampAssignerSupplier.Context
  ): TimestampAssigner[T] = {
    new ExtractorTimestampAssigner(extractor)
  }
}
