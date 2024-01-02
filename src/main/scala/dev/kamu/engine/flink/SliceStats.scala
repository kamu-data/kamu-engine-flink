package dev.kamu.engine.flink

import java.nio.file.Path
import java.time.Instant
import java.util.Scanner

case class SliceStats(
  lastWatermark: Option[Instant],
  numRecords: Long
)

object SliceStats {
  def read(path: Path): SliceStats = {
    val reader = new Scanner(path)

    val sRowCount = reader.nextLine()
    val sLastWatermark = reader.nextLine()

    val lLastWatermark = sLastWatermark.toLong

    reader.close()

    SliceStats(
      lastWatermark =
        if (lLastWatermark == Long.MinValue) None
        else Some(Instant.ofEpochMilli(lLastWatermark)),
      numRecords = sRowCount.toLong
    )
  }
}
