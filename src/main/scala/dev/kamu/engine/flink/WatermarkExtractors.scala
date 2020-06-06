package dev.kamu.engine.flink

import java.sql.Timestamp
import java.time.Instant

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

class InOrderWatermark[T <: Product](pos: Int)
    extends AssignerWithPunctuatedWatermarks[T] {

  private var lastWatermark = Long.MinValue

  override def extractTimestamp(
    element: T,
    previousElementTimestamp: Long
  ): Long = element.productElement(pos).asInstanceOf[Timestamp].getTime

  override def checkAndGetNextWatermark(
    lastElement: T,
    extractedTimestamp: Long
  ): Watermark =
    if (lastWatermark < extractedTimestamp) {
      lastWatermark = extractedTimestamp
      new Watermark(extractedTimestamp)
    } else null
}

class EventTimeFromRowWatermark(extract: Row => Long)
    extends AssignerWithPunctuatedWatermarks[Row] {
  override def extractTimestamp(
    element: Row,
    previousElementTimestamp: Long
  ): Long = extract(element)

  override def checkAndGetNextWatermark(
    lastElement: Row,
    extractedTimestamp: Long
  ) = new Watermark(extractedTimestamp)
}

class BoundedOutOfOrderWatermark[T](
  other: AssignerWithPunctuatedWatermarks[T],
  maxOutOfOrderness: Long
) extends AssignerWithPunctuatedWatermarks[T] {

  private val logger = LoggerFactory.getLogger(getClass)

  var lastWatermark: Long = Long.MinValue

  override def extractTimestamp(
    element: T,
    previousElementTimestamp: Long
  ): Long = {
    val ts = other.extractTimestamp(element, previousElementTimestamp)
    if (ts < lastWatermark) {
      logger.warn(
        s"Event is below current watermark (${Instant.ofEpochMilli(ts)} < ${Instant
          .ofEpochMilli(lastWatermark)}) and will be discarded: {}",
        element
      )
    }
    ts
  }

  override def checkAndGetNextWatermark(
    lastElement: T,
    extractedTimestamp: Long
  ): Watermark = {
    val wm = other.checkAndGetNextWatermark(lastElement, extractedTimestamp)
    val adjustedTime =
      if (wm == null) Long.MinValue else wm.getTimestamp - maxOutOfOrderness
    if (adjustedTime > lastWatermark) {
      lastWatermark = adjustedTime
      new Watermark(adjustedTime)
    } else null
  }
}

class LoggedWatermark[T](
  other: AssignerWithPunctuatedWatermarks[T]
) extends AssignerWithPunctuatedWatermarks[T] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def extractTimestamp(
    element: T,
    previousElementTimestamp: Long
  ): Long = {
    val ts = other.extractTimestamp(element, previousElementTimestamp)
    logger.debug("Extracted timestamp {} for {}", ts, element)
    ts
  }

  override def checkAndGetNextWatermark(
    lastElement: T,
    extractedTimestamp: Long
  ): Watermark = {
    val wm = other.checkAndGetNextWatermark(lastElement, extractedTimestamp)
    logger.debug("Setting watermark {} for {}", wm, lastElement)
    wm
  }
}

// DOC: https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamp_extractors.html
// DOC: https://stackoverflow.com/questions/55392857/why-flink-does-not-drop-late-data
object BoundedOutOfOrderWatermark {
  def forTuple[T <: Product](
    pos: Int,
    maxOutOfOrderness: Duration
  ): BoundedOutOfOrderWatermark[T] =
    apply(new InOrderWatermark[T](pos), maxOutOfOrderness)

  def forRow(
    extract: Row => Long,
    maxOutOfOrderness: Duration
  ): BoundedOutOfOrderWatermark[Row] =
    apply(new EventTimeFromRowWatermark(extract), maxOutOfOrderness)

  def apply[T](
    other: AssignerWithPunctuatedWatermarks[T],
    maxOutOfOrderness: Duration
  ) = new BoundedOutOfOrderWatermark(other, maxOutOfOrderness.toMillis)
}
