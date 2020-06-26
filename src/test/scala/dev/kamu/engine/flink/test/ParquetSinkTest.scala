package dev.kamu.engine.flink.test

import java.sql.Timestamp

import com.sksamuel.avro4s.ScalePrecisionRoundingMode
import dev.kamu.core.utils.fs.{Temp, _}
import dev.kamu.engine.flink.{AvroConverter, ParuqetSink, SchemaConverter}
import org.apache.flink.formats.parquet.ParquetRowInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.flink.core.fs.{Path => FlinkPath}
import org.apache.flink.types.Row
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import scala.math.BigDecimal.RoundingMode

case class Transaction(
  event_time: Timestamp,
  description: String,
  amount: BigDecimal,
  price: BigDecimal
)

class ParquetSinkTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers {

  val fileSystem = FileSystem.get(new Configuration())

  def getParquetSchema(path: Path): MessageType = {
    val file = HadoopInputFile.fromPath(path, new Configuration())
    val reader = ParquetFileReader.open(file)
    val messageType = reader.getFileMetaData.getSchema
    reader.close()
    messageType
  }

  test("Parquet sink") {
    Temp.withRandomTempDir(fileSystem, "kamu-engine-flink") { tempDir =>
      implicit val sp =
        ScalePrecisionRoundingMode(4, 18, RoundingMode.UNNECESSARY)

      val filePath = tempDir.resolve("1.parquet")
      val filePath2 = tempDir.resolve("2.parquet")

      ParquetHelpers.write(
        filePath,
        Seq(
          Transaction(ts(1), "A", BigDecimal("10.00"), BigDecimal("100.00")),
          Transaction(ts(2), "B", BigDecimal("20.00"), BigDecimal("100.00")),
          Transaction(ts(3), "C", BigDecimal("30.00"), BigDecimal("100.00"))
        )
      )

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tEnv = StreamTableEnvironment.create(env)

      env.setParallelism(1)

      val messageType = getParquetSchema(filePath)

      val inputFormat = new ParquetRowInputFormat(
        new FlinkPath(filePath.toUri.getPath),
        messageType
      )

      val stream = env
        .readFile(
          inputFormat,
          filePath.toUri.toString
        )(inputFormat.getProducedType)

      val table =
        tEnv.fromDataStream(
          stream,
          'event_time,
          'description,
          'amount,
          'price
        )

      tEnv.createTemporaryView("test", table)

      val result = tEnv.sqlQuery(
        """
           SELECT
             event_time,
             description,
             amount,
             price,
             CAST(amount * price as DECIMAL(18, 4)) as `value`
           FROM test
        """
      )

      val resultStream = result.toAppendStream[Row]
      resultStream.print()

      println(s"Input parquet schema: \n$messageType")
      println(s"Input stream schema: \n${stream.dataType}")

      println(s"Input table schema: \n${table.getSchema}")

      println(s"Result table schema: \n${result.getSchema}")
      println(s"Result stream schema: \n${resultStream.dataType}")

      val avroSchema = SchemaConverter.convert(result.getSchema)
      println(s"Result Avro schema:\n${avroSchema.toString(true)}")

      val avroConverter = new AvroConverter(avroSchema.toString())
      val avroStream = resultStream
        .map(r => avroConverter.convertRowToAvroRecord(r))

      avroStream.addSink(
        new ParuqetSink(
          avroSchema.toString(),
          filePath2.toUri.getPath,
          true
        )
      )

      env.execute()

      //println(tempDir)
      //readLine(">>>")

      val resultMessageType = getParquetSchema(filePath2)
      resultMessageType shouldEqual MessageTypeParser.parseMessageType(
        """
          message Row {
            optional int64 event_time (TIMESTAMP_MILLIS);
            optional binary description (UTF8);
            optional binary amount (DECIMAL(18,4));
            optional binary price (DECIMAL(18,4));
            optional binary value (DECIMAL(18,4));
          }
          """
      )
    }
  }

}
