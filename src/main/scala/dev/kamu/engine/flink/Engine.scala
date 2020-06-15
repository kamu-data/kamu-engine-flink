package dev.kamu.engine.flink

import java.io.File
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.{ExecuteQueryRequest, ExecuteQueryResult}
import dev.kamu.core.utils.Clock
import dev.kamu.core.utils.fs._
import org.apache.flink.api.common.JobStatus
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.{Path => FlinkPath}
import org.apache.flink.formats.parquet.ParquetRowInputFormat
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.source.{
  ContinuousFileReaderOperator,
  FileProcessingMode
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.slf4j.LoggerFactory
import spire.math.interval.{Closed, Open, Unbound}
import spire.math.{All, Empty, Interval}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.sys.process.Process

case class InputSlice(
  dataStream: DataStream[Row],
  dataSlice: DataSlice,
  markerPath: Path,
  statsPath: Path
)

case class SliceStats(
  hash: String,
  numRecords: Int
)

class Engine(
  fileSystem: FileSystem,
  systemClock: Clock,
  env: StreamExecutionEnvironment,
  tEnv: StreamTableEnvironment
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def executeQueryExtended(request: ExecuteQueryRequest): ExecuteQueryResult = {
    if (request.source.transformEngine != "flink")
      throw new RuntimeException(
        s"Invalid engine: ${request.source.transformEngine}"
      )

    val transform =
      yaml.load[TransformKind.Flink](request.source.transform.toConfig)

    val markersPath =
      request.datasetLayouts(request.datasetID.toString).checkpointsDir

    fileSystem.mkdirs(markersPath)

    val inputSlices =
      prepareInputSlices(
        typedMap(request.inputSlices),
        typedMap(request.datasetVocabs),
        typedMap(request.datasetLayouts),
        markersPath
      )

    val resultTable = executeQuery(
      request.datasetID,
      inputSlices,
      typedMap(request.datasetVocabs),
      transform
    )

    logger.info("Result schema:\n{}", resultTable.getSchema)
    val resultStream = resultTable.toAppendStream[Row]

    // Computes row count and data hash
    val resultStatsPath = markersPath.resolve("output-stats")
    resultStream.addSink(new StatsSink(resultStatsPath.toUri.getPath))

    // Convert to Avro so we can then save in Parquet :(
    val avroSchema = SchemaConverter.convert(resultTable.getSchema)
    logger.info("Result schema in Avro format:\n{}", avroSchema.toString(true))

    val avroConverter = new AvroConverter(avroSchema.toString())
    val avroStream =
      resultStream.map(r => avroConverter.convertRowToAvroRecord(r))

    val dataFilePath = request
      .datasetLayouts(request.datasetID.toString)
      .dataDir
      .resolve(
        systemClock
          .instant()
          .toString
          .replaceAll("[:.]", "") + ".snappy.parquet"
      )

    avroStream.addSink(
      new ParuqetSink(
        avroSchema.toString(),
        dataFilePath.toUri.getPath
      )
    )

    processAvailableAndStopWithSavepoint(
      inputSlices,
      request.datasetLayouts(request.datasetID.toString).checkpointsDir
    )

    val stats = gatherStats(inputSlices, request.datasetID, resultStatsPath)

    // TODO: Compute hash, interval and num records
    val block = MetadataBlock(
      prevBlockHash = "",
      // TODO: Current time? Min of input times? Require to propagate in computations?
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = stats(request.datasetID).hash,
          interval = Interval.point(systemClock.instant()),
          numRecords = stats(request.datasetID).numRecords
        )
      ),
      inputSlices = request.source.inputs.map(
        i =>
          inputSlices(i.id).dataSlice.copy(
            hash = stats(i.id).hash,
            numRecords = stats(i.id).numRecords
          )
      )
    )

    ExecuteQueryResult(
      block = block,
      dataFileName =
        if (fileSystem.exists(dataFilePath)) Some(dataFilePath.getName)
        else None
    )
  }

  private def executeQuery(
    datasetID: DatasetID,
    inputSlices: Map[DatasetID, InputSlice],
    datasetVocabs: Map[DatasetID, DatasetVocabulary],
    transform: TransformKind.Flink
  ): Table = {
    // Prepare watermarks
    val watermarks = transform.watermarks.map(w => (w.id, w)).toMap

    // Setup inputs
    for ((inputID, slice) <- inputSlices) {
      val inputVocab = datasetVocabs(inputID).withDefaults()
      val watermark = watermarks.get(inputID)

      val eventTimeColumn = inputVocab.eventTimeColumn.get
      val eventTimePos = FieldInfoUtils
        .getFieldNames(slice.dataStream.dataType)
        .indexOf(eventTimeColumn)

      if (eventTimePos < 0)
        throw new Exception(
          s"Event time column not found: $eventTimeColumn"
        )

      val stream = slice.dataStream.assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark.forRow(
          _.getField(eventTimePos).asInstanceOf[Timestamp].getTime,
          watermark.flatMap(_.maxLateBy).getOrElse(Duration.Zero)
        )
      )

      val columns = FieldInfoUtils
        .getFieldNames(slice.dataStream.dataType)
        .map({
          case `eventTimeColumn` => s"$eventTimeColumn.rowtime"
          case other             => other
        })

      val expressions =
        ExpressionParser.parseExpressionList(columns.mkString(", ")).asScala

      val table = tEnv
        .fromDataStream(stream, expressions: _*)
        .dropColumns(inputVocab.systemTimeColumn.get)

      logger.info(
        "Registered input {} with schema:\n{}",
        inputID,
        table.getSchema
      )

      tEnv.createTemporaryView(s"`$inputID`", table)

      watermark.map(_.primaryKey).getOrElse(Vector.empty) match {
        case Vector() =>
        case Vector(pk) =>
          tEnv.registerFunction(
            inputID.toString,
            table.createTemporalTableFunction(eventTimeColumn, pk)
          )
          logger.info(
            "Registered input {} as temporal table with PK: {}",
            inputID,
            pk
          )
        case _ =>
          throw new NotImplementedError(
            "Composite primary keys are not supported by Flink"
          )
      }
    }

    // Setup transform
    for (step <- transform.queries) {
      val alias = step.alias.getOrElse(datasetID.toString)
      val table = tEnv.sqlQuery(step.query)
      tEnv.createTemporaryView(s"`$alias`", table)
      //table.toAppendStream[Row].addSink(new SnitchSink(alias))
    }

    // Get result
    val result = tEnv.sqlQuery(s"SELECT * FROM `$datasetID`")

    val resultVocab = datasetVocabs(datasetID).withDefaults()

    if (result.getSchema
          .getTableColumn(resultVocab.systemTimeColumn.get)
          .isPresent)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${resultVocab.systemTimeColumn.get}"
      )

    if (!result.getSchema
          .getTableColumn(resultVocab.eventTimeColumn.get)
          .isPresent())
      throw new Exception(
        s"Event time column ${resultVocab.eventTimeColumn.get} was not found amongst: " +
          result.getSchema.getTableColumns.asScala.map(_.getName).mkString(", ")
      )

    tEnv.sqlQuery(
      s"SELECT CAST('${systemClock.timestamp()}' as TIMESTAMP) as `system_time`, * FROM `$datasetID`"
    )
  }

  private def processAvailableAndStopWithSavepoint(
    inputSlices: Map[DatasetID, InputSlice],
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
      inputSlices.values.forall(i => fileSystem.exists(i.markerPath))
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

      logger.info("Self-canceling job {}", job.getJobID.toString)
      val out = Process(
        Seq(
          "flink",
          "cancel",
          "-s",
          checkpointDir.toUri.getPath,
          job.getJobID.toString
        )
      ).!!

      println(s"OUTPUT: $out")
    } finally {
      inputSlices.values.foreach(i => fileSystem.delete(i.markerPath, false))
    }
  }

  private def prepareInputSlices(
    inputSlices: Map[DatasetID, DataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary],
    inputLayouts: Map[DatasetID, DatasetLayout],
    markersPath: Path
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice =
          prepareInputSlice(
            id,
            slice,
            inputVocabs(id).withDefaults(),
            inputLayouts(id),
            markersPath
          )
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    id: DatasetID,
    slice: DataSlice,
    vocab: DatasetVocabulary,
    layout: DatasetLayout,
    markersPath: Path
  ): InputSlice = {
    val markerPath = markersPath.resolve(s"input-marker-$id")
    val statsPath = markersPath.resolve(s"input-stats-$id")

    // TODO: use schema from metadata
    val stream =
      sliceData(
        openStream(id, layout.dataDir, markerPath),
        slice.interval,
        vocab
      )

    //stream.addSink(new SnitchSink(id.toString))

    // Computes hash and count rows
    stream.addSink(new StatsSink(statsPath.toUri.getPath))

    InputSlice(
      dataStream = stream,
      dataSlice = slice,
      markerPath = markerPath,
      statsPath = statsPath
    )
  }

  private def sliceData(
    stream: DataStream[Row],
    interval: Interval[Instant],
    vocab: DatasetVocabulary
  ): DataStream[Row] = {
    interval match {
      case Empty() =>
        stream.filter(_ => false)
      case All() =>
        stream
      case _ =>
        val schema = stream.dataType.asInstanceOf[RowTypeInfo]
        val systemTimeColumn = schema.getFieldIndex(vocab.systemTimeColumn.get)

        val dfLower = interval.lowerBound match {
          case Unbound() =>
            stream
          case Open(x) =>
            val ts = Timestamp.from(x)
            stream.filter(
              r => {
                r.getField(systemTimeColumn)
                  .asInstanceOf[Timestamp]
                  .compareTo(ts) > 0
              }
            )
          case Closed(x) =>
            val ts = Timestamp.from(x)
            stream.filter(
              r => {
                r.getField(systemTimeColumn)
                  .asInstanceOf[Timestamp]
                  .compareTo(ts) >= 0
              }
            )
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }

        interval.upperBound match {
          case Unbound() =>
            dfLower
          case Open(x) =>
            val ts = Timestamp.from(x)
            stream.filter(
              r => {
                r.getField(systemTimeColumn)
                  .asInstanceOf[Timestamp]
                  .compareTo(ts) < 0
              }
            )
          case Closed(x) =>
            val ts = Timestamp.from(x)
            stream.filter(
              r => {
                r.getField(systemTimeColumn)
                  .asInstanceOf[Timestamp]
                  .compareTo(ts) <= 0
              }
            )
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }
    }
  }

  private def typedMap[T](m: Map[String, T]): Map[DatasetID, T] = {
    m.map {
      case (id, value) => (DatasetID(id), value)
    }
  }

  private def gatherStats(
    inputSlices: Map[DatasetID, InputSlice],
    outputID: DatasetID,
    outputStatsPath: Path
  ): Map[DatasetID, SliceStats] = {
    val pathMap = inputSlices.mapValues(_.statsPath) ++ Map(
      outputID -> outputStatsPath
    )

    val stats = pathMap.mapValues(readStats).view.force

    // Cleanup
    pathMap.values.foreach(p => fileSystem.delete(p, false))

    stats
  }

  private def readStats(path: Path): SliceStats = {
    try {
      val reader = new Scanner(new File(path.toUri.getPath))

      val sRowCount = reader.nextLine()
      val hash = reader.nextLine()

      reader.close()

      SliceStats(
        hash = hash,
        numRecords = sRowCount.toInt
      )
    } catch {
      case e: Exception =>
        println(s"Error while reading stats file: $path $e")
        logger.error(s"Error while reading stats file: $path", e)
        throw e
    }
  }

  private def openStream(
    datasetID: DatasetID,
    path: Path,
    markerPath: Path
  ): DataStream[Row] = {
    // TODO: Ignoring schema evolution
    val schema = getSchemaFromFile(findFirstParquetFile(path).get)
    logger.debug("Using following schema:\n{}", schema)

    val messageType = MessageTypeParser.parseMessageType(schema.toString)

    val inputFormat = new ParquetRowInputFormat(
      new FlinkPath(path.toUri.getPath),
      messageType
    )

    /*env.readFile[Row](
      inputFormat,
      path.toUri.getPath,
      FileProcessingMode.PROCESS_CONTINUOUSLY,
      500
    )(inputFormat.getProducedType)
     */

    new DataStream[Row](
      createFileInput(
        inputFormat,
        inputFormat.getProducedType,
        datasetID.toString,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        1000,
        markerPath
      )
    )
  }

  private def getSchemaFromFile(path: Path): MessageType = {
    logger.debug("Loading schema from: {}", path)
    val file = HadoopInputFile.fromPath(path, new Configuration())
    val reader = ParquetFileReader.open(file)
    val schema = reader.getFileMetaData.getSchema
    reader.close()
    schema
  }

  private def findFirstParquetFile(path: Path): Option[Path] = {
    for (f <- fileSystem.listStatus(path)) {
      if (f.getPath.getName.endsWith(".parquet"))
        return Some(f.getPath)
    }
    None
  }

  // TODO: This env method is overridden to customize file reader behavior
  private def createFileInput[T](
    inputFormat: FileInputFormat[T],
    typeInfo: TypeInformation[T],
    sourceName: String,
    monitoringMode: FileProcessingMode,
    interval: Long,
    markerPath: Path
  ): DataStreamSource[T] = {
    val monitoringFunction =
      new CustomFileMonitoringFunction[T](
        inputFormat,
        monitoringMode,
        env.getParallelism,
        interval
      )

    val reader =
      new CustomFileReaderOperator[T](inputFormat, markerPath.toUri.getPath)
    //val reader = new ContinuousFileReaderOperator[T](inputFormat)

    val source = env.getJavaEnv
      .addSource(monitoringFunction, sourceName)
      .transform("Split Reader: " + sourceName, typeInfo, reader)

    new DataStreamSource(source)
  }
}
