package dev.kamu.engine.flink

import java.nio.file.Path
import java.time.Instant
import java.util.Scanner
import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{
  TransformRequest,
  TransformRequestInput,
  TransformResponse
}
import dev.kamu.core.utils.fs._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.JobStatus
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.formats.avro.typeutils.{
  AvroSchemaConverter,
  GenericRecordAvroTypeInfo
}
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormatKamu
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverterKamu
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{$, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.{
  ExternalTypeInfo,
  InternalTypeInfo
}
import org.apache.flink.types.{Row, RowKind}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.core.execution.SavepointFormatType
import org.apache.flink.formats.avro.RowDataToAvroConverters
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.data.conversion.RowRowConverter
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConverters._

class RawQueryEngine(
  env: StreamExecutionEnvironment,
  tEnv: StreamTableEnvironment
) {
  private val outputQueryAlias = "__output__"
  private val logger = LoggerFactory.getLogger(classOf[Transform])
  private val hadoopConfig = new org.apache.hadoop.conf.Configuration()

  def executeRawQuery(
    request: RawQueryRequest
  ): RawQueryResponse = {
    val transform = loadTransform(request.transform)

    val input =
      loadInput(
        request.inputDataPaths
      )

    val resultStatsPath =
      request.outputDataPath.getParent.resolve("output.stats")

    val resultStream = executeTransform(
      input,
      transform
    ).withStats(outputQueryAlias, resultStatsPath, flushOnClose = true)

    // Convert Row -> RowData
    val resultStreamDataType =
      resultStream.dataType.asInstanceOf[ExternalTypeInfo[Row]].getDataType

    val converterRowData =
      RowRowConverter.create(resultStreamDataType)

    val resultStreamRowData =
      resultStream.map(row => converterRowData.toInternal(row))(
        InternalTypeInfo.of(resultStreamDataType.getLogicalType)
      )

    // Convert RowData -> Avro GenericRecord
    val avroSchema =
      AvroSchemaConverter.convertToSchema(resultStreamDataType.getLogicalType)
    logger.info(s"Result schema in Avro format:\n${avroSchema.toString(true)}")

    val converterAvro = RowDataToAvroConverters.createConverter(
      resultStreamDataType.getLogicalType
    )

    val resultStreamAvro = resultStreamRowData.map(
      row => converterAvro.convert(avroSchema, row).asInstanceOf[GenericRecord]
    )(new GenericRecordAvroTypeInfo(avroSchema))

    resultStreamAvro.addSink(
      new ParuqetSink(
        avroSchema.toString(),
        request.outputDataPath.toString,
        true
      )
    )

    // Run processing
    logger.info(s"Execution plan: ${env.getExecutionPlan}")
    env.execute()

    val resultStats = SliceStats.read(resultStatsPath)
    RawQueryResponse.Success(numRecords = resultStats.numRecords)
  }

  def executeTransform(
    input: DataStream[RowData],
    transform: Transform.Sql
  ): DataStream[Row] = {
    // Setup input

    // See: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromdatastream
    val tableSchemaBuilder = org.apache.flink.table.api.Schema
      .newBuilder()

    val table = tEnv
      .fromDataStream(
        input
          .withDebugLogging("input"),
        tableSchemaBuilder.build()
      )

    logger.info(
      s"Registered input with schema:\n${table.getResolvedSchema}"
    )

    tEnv.createTemporaryView(s"`input`", table)

    // Setup transform
    for (step <- transform.queries.get) {
      val alias = step.alias.getOrElse(outputQueryAlias)

      tEnv.createTemporaryView(s"`$alias`", tEnv.sqlQuery(step.query))
      val queryTable = tEnv.from(s"`$alias`")

      logger.info(s"Created view '${alias}' for query:\n${step.query}")

      // Log intermediate results
      queryTable.toChangelogStream.withDebugLogging(alias)
    }

    // Get result
    val rawResult = tEnv.from(s"`$outputQueryAlias`")
    logger.info("Raw result schema:\n{}", rawResult.getResolvedSchema)

    rawResult
      .toDataStream[Row](classOf[Row])
      .withDebugLogging(s"${outputQueryAlias}::final")
  }

  private def loadInput(
    inputPaths: Vector[Path]
  ): DataStream[RowData] = {
    val parquetSchema = getSchemaFromFile(inputPaths(0))
    logger.info(
      s"Input has parquet schema:\n${parquetSchema}"
    )

    val rowType = ParquetSchemaConverterKamu.convertToRowType(parquetSchema)
    logger.info(s"Converted RowType:\n${rowType}")

    val typeInfo = InternalTypeInfo.of(rowType)

    val inputFormat = new ParquetColumnarRowInputFormatKamu[FileSourceSplit](
      hadoopConfig,
      rowType,
      typeInfo,
      500,
      true,
      true
    )

    env.addSource(
      new ParquetFilesStreamSourceFunction(
        "input",
        inputPaths.map(_.toString),
        inputFormat,
        _ => RowKind.INSERT,
        _ => 0,
        None,
        Vector.empty,
        true,
        None
      )
    )(typeInfo)
  }

  private def getSchemaFromFile(path: Path): MessageType = {
    logger.debug("Loading schema from: {}", path)
    val file = HadoopInputFile.fromPath(
      new org.apache.hadoop.fs.Path(path.toUri),
      new org.apache.hadoop.conf.Configuration()
    )
    val reader = ParquetFileReader.open(file)
    val schema = reader.getFileMetaData.getSchema
    reader.close()
    schema
  }

  def loadTransform(raw: Transform): Transform.Sql = {
    if (raw.engine != "flink")
      throw new RuntimeException(s"Unsupported engine: ${raw.engine}")

    raw.asInstanceOf[Transform.Sql]
  }

  implicit class StreamHelpers[T](s: DataStream[T]) {

    def withStats(
      id: String,
      path: Path,
      flushOnClose: Boolean = false
    ): DataStream[T] = {
      s.transform(
        "stats",
        new StatsOperator(id, path.toUri.getPath, flushOnClose)
      )(s.dataType)
    }

    def withDebugLogging(id: String): DataStream[T] = {
      if (logger.isDebugEnabled) {
        s.transform(
          "snitch",
          new SnitchOperator(id)
        )(s.dataType)
      } else {
        s
      }
    }

  }
}
