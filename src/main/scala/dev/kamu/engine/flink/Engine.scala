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
  ExecuteQueryRequestInput,
  ExecuteQueryRequest,
  ExecuteQueryResponse
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
import org.apache.flink.types.Row
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

case class InputStream(
  inputDef: ExecuteQueryRequestInput,
  dataStream: DataStream[RowData],
  markerPath: Path,
  vocab: DatasetVocabulary
)

case class SliceStats(
  lastWatermark: Option[Instant],
  numRecords: Long
)

class Engine(
  env: StreamExecutionEnvironment,
  tEnv: StreamTableEnvironment
) {
  private val outputQueryAlias = "__output__"
  private val logger = LoggerFactory.getLogger(classOf[Engine])
  private val hadoopConfig = new org.apache.hadoop.conf.Configuration()

  def executeRequest(
    requestRaw: ExecuteQueryRequest
  ): ExecuteQueryResponse = {
    val request = requestRaw.withVocabDefaults()
    val transform = loadTransform(request.transform)

    File(request.newCheckpointPath).createDirectories()

    val inputs =
      openInputs(
        request.queryInputs,
        request.prevCheckpointPath,
        request.newCheckpointPath
      )

    val resultStream = executeTransform(
      inputs,
      transform,
      request.systemTime,
      request.nextOffset,
      request.vocab.withDefaults()
    ).withStats(
      outputQueryAlias,
      request.newCheckpointPath.resolve(
        s"${request.datasetId.toMultibase()}.stats"
      )
    )

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
        request.newDataPath.toString
      )
    )

    // Run processing

    logger.info(s"Execution plan: ${env.getExecutionPlan}")

    processAvailableAndStopWithSavepoint(
      inputs,
      request.newCheckpointPath
    )

    val stats = gatherStats(
      request.datasetId :: inputs.map(_.inputDef.datasetId).toList,
      request.newCheckpointPath
    )

    val outputStats = stats(request.datasetId)

    ExecuteQueryResponse.Success(
      newOffsetInterval =
        if (outputStats.numRecords > 0)
          Some(
            OffsetInterval(
              request.nextOffset,
              request.nextOffset + outputStats.numRecords - 1
            )
          )
        else None,
      newWatermark = outputStats.lastWatermark
    )
  }

  def executeTransform(
    inputs: Seq[InputStream],
    transform: Transform.Sql,
    systemTime: Instant,
    startOffset: Long,
    outputVocab: DatasetVocabulary
  ): DataStream[Row] = {
    // Temporal table helpers
    val temporalTables =
      transform.temporalTables
        .getOrElse(Vector.empty)
        .map(t => (t.name, t))
        .toMap

    def maybeRegisterTemporalTable(
      table: Table,
      alias: String,
      eventTimeColumn: String
    ): Unit = {
      val tt = temporalTables
        .get(alias)

      if (tt.isDefined) {
        tt.get.primaryKey match {
          case Vector() =>
            throw new NotImplementedError(
              "Temporal table does not define a primary key"
            )
          case Vector(pk) =>
            val tableFunc =
              table.createTemporalTableFunction($(eventTimeColumn), $(pk))

            tEnv.createTemporarySystemFunction(alias, tableFunc)
            logger.info(
              "Registered temporal table '{}' with PK: {}",
              alias: Any,
              pk: Any
            )
          case _ =>
            throw new NotImplementedError(
              "Composite primary keys are not supported by Flink"
            )
        }
      }
    }

    // Setup inputs
    for (input <- inputs) {
      val queryAlias = input.inputDef.queryAlias

      // See: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromdatastream
      val tableSchemaBuilder = org.apache.flink.table.api.Schema
        .newBuilder()
        .watermark(input.vocab.eventTimeColumn.get, "SOURCE_WATERMARK()")

      // TODO: FOR SYSTEM_TIME AS OF join requires PK to be set on the temporal table which is not convenient
      // perhaps in future we can deduce PKs from the queries
      temporalTables
        .get(queryAlias)
        .foreach(
          tt => tableSchemaBuilder.primaryKey(tt.primaryKey.toList.asJava)
        )

      val table = tEnv
        .fromDataStream(
          input.dataStream
            .withDebugLogging(queryAlias),
          tableSchemaBuilder.build()
        )

      logger.info(
        s"Registered input ${input.inputDef.datasetAlias} (${input.inputDef.datasetId}) as '${queryAlias}' with schema:\n${table.getResolvedSchema}"
      )

      tEnv.createTemporaryView(s"`$queryAlias`", table)

      maybeRegisterTemporalTable(
        table.dropColumns($(input.vocab.systemTimeColumn.get)),
        queryAlias,
        input.vocab.eventTimeColumn.get
      )
    }

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

    // Validate user query result
    if (rawResult.getResolvedSchema
          .getColumn(outputVocab.offsetColumn.get)
          .isPresent)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${outputVocab.offsetColumn.get}"
      )

    if (rawResult.getResolvedSchema
          .getColumn(outputVocab.systemTimeColumn.get)
          .isPresent)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${outputVocab.systemTimeColumn.get}"
      )

    if (!rawResult.getResolvedSchema
          .getColumn(outputVocab.eventTimeColumn.get)
          .isPresent)
      throw new Exception(
        s"Event time column ${outputVocab.eventTimeColumn.get} was not found amongst: " +
          rawResult.getResolvedSchema.getColumns.asScala
            .map(_.getName)
            .mkString(", ")
      )

    val systemTimeStr = systemTime.toString.stripSuffix("Z").replace('T', ' ')

    // Add system columns
    val resultTable = tEnv
      .sqlQuery(
        s"SELECT cast(0 as BIGINT) as `offset`, CAST('${systemTimeStr}' as TIMESTAMP(3)) as `system_time`, * FROM `${outputQueryAlias}`"
      )

    logger.info("Final result schema:\n{}", resultTable.getSchema)

    resultTable
      .toDataStream[Row](classOf[Row])
      .assignOffsets(startOffset)
      .withDebugLogging(s"${outputQueryAlias}::final")
  }

  private def processAvailableAndStopWithSavepoint(
    inputSlices: Seq[InputStream],
    checkpointDir: Path
  ): Unit = {
    val job = env.executeAsync()

    def jobRunning(): Boolean = {
      job.getJobStatus.get match {
        case JobStatus.FAILED | JobStatus.FINISHED | JobStatus.CANCELED =>
          false
        case _ => true
      }
    }

    def inputsExhausted(): Boolean = {
      inputSlices.forall(i => File(i.markerPath).exists)
    }

    try {
      while (jobRunning() && !inputsExhausted()) {
        Thread.sleep(500)
      }

      if (!jobRunning()) {
        throw new RuntimeException(
          s"Job failed with status: ${job.getJobStatus.get}"
        )
      }

      logger.info(s"Self-canceling job ${job.getJobID.toString}")
      job
        .stopWithSavepoint(
          false,
          checkpointDir.toUri.toString,
          SavepointFormatType.CANONICAL
        )
        .join()
    } finally {
      inputSlices.foreach(
        i => File(i.markerPath).delete(swallowIOExceptions = true)
      )
    }
  }

  private def openInputs(
    inputs: Vector[ExecuteQueryRequestInput],
    prevCheckpointDir: Option[Path],
    newCheckpointDir: Path
  ): Vector[InputStream] = {
    inputs
      .map(input => {
        openInputStreamCheckpointable(
          input,
          prevCheckpointDir,
          newCheckpointDir
        )
      })
  }

  private def openInputStreamCheckpointable(
    input: ExecuteQueryRequestInput,
    prevCheckpointDir: Option[Path],
    newCheckpointDir: Path
  ): InputStream = {
    val markerPath =
      newCheckpointDir.resolve(s"${input.datasetId.toMultibase()}.marker")
    val prevStatsPath =
      prevCheckpointDir.map(
        _.resolve(s"${input.datasetId.toMultibase()}.stats")
      )
    val newStatsPath =
      newCheckpointDir.resolve(s"${input.datasetId.toMultibase()}.stats")

    val prevStats = prevStatsPath.map(readStats)

    val stream =
      openInputStream(
        input,
        prevStats.flatMap(_.lastWatermark),
        terminateWhenExhausted = false,
        Some(markerPath)
      )

    val streamWithStats = stream
      .withStats(input.queryAlias, newStatsPath)

    InputStream(
      inputDef = input,
      dataStream = streamWithStats,
      markerPath = markerPath,
      vocab = input.vocab.withDefaults()
    )
  }

  private def sliceData(
    stream: DataStream[RowData],
    interval: Option[OffsetInterval],
    vocab: DatasetVocabulary
  ): DataStream[RowData] = {
    val rowType =
      stream.dataType.asInstanceOf[InternalTypeInfo[RowType]].toRowType
    val offsetColumn = rowType.getFieldIndex(vocab.offsetColumn.get)

    interval match {
      case None => stream.filter(_ => false)
      case Some(iv) =>
        stream.filter(row => {
          val offset = row.getLong(offsetColumn)
          offset >= iv.start && offset <= iv.end
        })
    }
  }

  private def gatherStats(
    datasetIds: Seq[DatasetId],
    newCheckpointDir: Path
  ): Map[DatasetId, SliceStats] = {
    datasetIds
      .map(id => (id, newCheckpointDir.resolve(s"${id.toMultibase()}.stats")))
      .map { case (name, p) => (name, readStats(p)) }
      .toMap
  }

  private def readStats(path: Path): SliceStats = {
    try {
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
    } catch {
      case e: Exception =>
        logger.error(s"Error while reading stats file: $path", e)
        throw e
    }
  }

  private def openInputStream(
    input: ExecuteQueryRequestInput,
    prevWatermark: Option[Instant],
    terminateWhenExhausted: Boolean,
    exhaustedMarkerPath: Option[Path]
  ): DataStream[RowData] = {
    // TODO: Ignoring schema evolution
    // TODO: use schema from metadata
    val parquetSchema = getSchemaFromFile(input.schemaFile)
    logger.info(
      s"Dataset ${input.datasetAlias} (${input.datasetId}) has parquet schema:\n${parquetSchema}"
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

    val eventTimePos =
      parquetSchema.getFieldIndex(input.vocab.eventTimeColumn.get)

    if (eventTimePos < 0)
      throw new Exception(
        s"Event time column not found: ${input.vocab.eventTimeColumn.get}"
      )

    val stream = env.addSource(
      new ParquetFilesStreamSourceFunction(
        input.queryAlias,
        input.dataPaths.map(_.toString),
        inputFormat,
        row => row.getTimestamp(eventTimePos, Int.MaxValue).getMillisecond,
        prevWatermark,
        input.explicitWatermarks.map(_.eventTime),
        terminateWhenExhausted,
        exhaustedMarkerPath.map(_.toString)
      )
    )(typeInfo)

    // TODO: We often need to emit watermarks that do not originate from records
    // WatermarkStrategy makes it impossible, so the job of timestamp extraction and watermarking
    // is currently done by the source.
    /*val streamWithWatermarks = stream.assignTimestampsAndWatermarks(
      new MaxOutOfOrderWatermarkStrategy[RowData](
        row => row.getTimestamp(eventTimePos, Int.MaxValue).getMillisecond,
        scala.concurrent.duration.Duration.Zero
      )
    )*/

    sliceData(stream, input.offsetInterval, input.vocab)
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

    val sql = raw.asInstanceOf[Transform.Sql]

    sql.copy(
      queries =
        if (sql.query.isDefined) Some(Vector(SqlQueryStep(None, sql.query.get)))
        else sql.queries
    )
  }

  implicit class StreamHelpers(s: DataStream[Row]) {
    def assignOffsets(startOffset: Long): DataStream[Row] = {
      s.transform("offset", new OffsetOperator(startOffset))(s.dataType)
    }
  }

  implicit class StreamHelpers2[T](s: DataStream[T]) {

    def withStats(id: String, path: Path): DataStream[T] = {
      s.transform(
        "stats",
        new StatsOperator(id, path.toUri.getPath)
      )(s.dataType)
    }

    def withDebugLogging(id: String): DataStream[T] = {
      s.transform(
        "snitch",
        new SnitchOperator(id)
      )(s.dataType)
    }

  }
}
