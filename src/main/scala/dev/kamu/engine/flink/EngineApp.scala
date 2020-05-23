package dev.kamu.engine.flink

import dev.kamu.core.manifests.infra.TransformConfig
import dev.kamu.core.utils.ManualClock
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

object EngineApp {
  /*
  def executeTransform(data: Seq[Row], typeInfo: TypeInformation[Row]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream = env
      .addSource(
        new UnboundedFromElementsFunction[Row](
          typeInfo.createSerializer(env.getConfig),
          scala.collection.JavaConversions.asJavaCollection(data)
        )
      )(typeInfo)
      .assignTimestampsAndWatermarks(
        new EventTimeFromRowWatermark(
          _.getField(0).asInstanceOf[Timestamp].getTime
        )
      )

    val table =
      tEnv.fromDataStream(inputStream, 'event_time.rowtime, 'symbol, 'price)

    table.printSchema()
    tEnv.createTemporaryView("Tickers", table)

    val resultStream = tEnv
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

    val avroSchema = SchemaConverter.convert(resultStream.dataType)
    val avroConverter = new AvroConverter(avroSchema.toString())
    val avroStream =
      resultStream.map(
        r => avroConverter.convertRowToAvroRecord(r)
      )

    /*val parquetSink = StreamingFileSink
      .forBulkFormat(
        new Path("data/result"),
        ParquetAvroWriters.forGenericRecord(avroSchema)
      )
      .build()*/

    avroStream.addSink(new ParuqetSink(avroSchema.toString()))

    val f = new File(".status")
    if (f.exists())
      f.delete()

    //env.execute()
    val job = env.executeAsync()
    while (job.getJobStatus.get() == JobStatus.RUNNING && !f.exists()) {
      println("zzzzzzz")
      Thread.sleep(500)
    }
    if (job.getJobStatus.get == JobStatus.RUNNING)
      job.stopWithSavepoint(false, "savepoints").get()
  }*/

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(getClass.getName)

    val config = TransformConfig.load()
    if (config.tasks.isEmpty) {
      logger.warn("No tasks specified")
      return
    }

    logger.info(s"Running with config: $config")

    val fileSystem = FileSystem.get(new Configuration())
    val systemClock = new ManualClock()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    for (taskConfig <- config.tasks) {
      systemClock.advance()
      logger.info(s"Processing dataset: ${taskConfig.datasetID}")

      val engine = new Engine(fileSystem, systemClock, env, tEnv)
      engine.executeQueryExtended(taskConfig)

      logger.info(s"Done processing dataset: ${taskConfig.datasetID}")
    }

    logger.info("Finished")
  }
}
