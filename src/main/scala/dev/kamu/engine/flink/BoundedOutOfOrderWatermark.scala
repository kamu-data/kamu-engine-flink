package dev.kamu.engine.flink

import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.types.Row

import scala.concurrent.duration.Duration

class BoundedOutOfOrderWatermark[T](
  extractor: T => Long,
  maxOutOfOrderness: Long
) extends AssignerWithPunctuatedWatermarks[T] {

  override def extractTimestamp(
    element: T,
    previousElementTimestamp: Long
  ): Long = {
    extractor(element)
  }

  override def checkAndGetNextWatermark(
    lastElement: T,
    extractedTimestamp: Long
  ): Watermark = {
    if (extractedTimestamp == Long.MinValue)
      null
    else
      new Watermark(extractedTimestamp - maxOutOfOrderness)
  }
}

// DOC: https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html
// DOC: https://stackoverflow.com/questions/55392857/why-flink-does-not-drop-late-data
object BoundedOutOfOrderWatermark {
  def forTuple[T <: Product](
    pos: Int,
    maxOutOfOrderness: Duration
  ): BoundedOutOfOrderWatermark[T] =
    apply(
      _.productElement(pos).asInstanceOf[Timestamp].getTime,
      maxOutOfOrderness
    )

  def forRow(
    extractor: Row => Long,
    maxOutOfOrderness: Duration
  ): BoundedOutOfOrderWatermark[Row] =
    apply(extractor, maxOutOfOrderness)

  def apply[T](
    extractor: T => Long,
    maxOutOfOrderness: Duration
  ): BoundedOutOfOrderWatermark[T] =
    new BoundedOutOfOrderWatermark(extractor, maxOutOfOrderness.toMillis)
}
