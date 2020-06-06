package dev.kamu.engine.flink

import java.sql.Timestamp

import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object EngineAppX {

  def mainz(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputPath = "/opt/engine/workspace/data/tickers_csv"
    val inputFormat = new RowCsvInputFormat(
      new Path(inputPath),
      Array(Types.SQL_TIMESTAMP(), Types.STRING(), Types.INT())
    )

    val inputStream = env
      .readFile[Row](
        inputFormat,
        inputPath,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        interval = 1000
      )(inputFormat.getProducedType)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Row](Time.days(1)) {
          override def extractTimestamp(row: Row): Long = {
            row.getField(0).asInstanceOf[Timestamp].getTime
          }
        }
      )

    inputStream.print()

    val table =
      tEnv.fromDataStream(inputStream, 'event_time.rowtime, 'symbol, 'price)

    table.printSchema()

    tEnv.createTemporaryView("Tickers", table)

    tEnv
      .sqlQuery(
        """
        SELECT
          TUMBLE_START(event_time, INTERVAL '1' DAY) as event_time,
          symbol as symbol,
          min(price) as `min`,
          max(price) as `max`
        FROM Tickers
        GROUP BY TUMBLE(event_time, INTERVAL '1' DAY), symbol
        """
      )
      .toAppendStream[Row]
      .print()

    env.execute()

    /*val job = env.executeAsync()
    for (_ <- 1 to 10) {
      println(job.getJobStatus.get())
      Thread.sleep(500)
    }
    job
      .stopWithSavepoint(
        false,
        "savepoints"
      )
      .get()*/
  }
}
