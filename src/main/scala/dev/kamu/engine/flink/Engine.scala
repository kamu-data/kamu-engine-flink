package dev.kamu.engine.flink

import java.nio.file.{Path, Paths}
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner

import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  InputDataSlice,
  Watermark
}
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
  FileProcessingMode,
  TimestampedFileInputSplit
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.logging.log4j.LogManager
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import spire.math.interval.{Closed, Open, Unbound}
import spire.math.{All, Empty, Interval}

import scala.collection.JavaConverters._
import scala.sys.process.Process

case class InputSlice(
  dataStream: DataStream[Row],
  interval: Interval[Instant],
  markerPath: Path
)

case class SliceStats(
  hash: String,
  lastWatermark: Option[Instant],
  numRecords: Long
)

class Engine(
  systemClock: Clock,
  env: StreamExecutionEnvironment,
  tEnv: StreamTableEnvironment
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def executeQueryExtended(request: ExecuteQueryRequest): ExecuteQueryResult = {
    if (request.source.transformEngine != "flink")
      throw new RuntimeException(
        s"Invalid engine: ${request.source.transformEngine}"
      )

    val transform =
      yaml.load[TransformKind.Flink](request.source.transform.toConfig)

    val checkpointsDir = Paths.get(request.checkpointsDir)

    File(checkpointsDir).createDirectories()

    val inputSlices =
      prepareInputSlices(
        typedMap(request.inputSlices),
        typedMap(request.datasetVocabs),
        typedMap(request.dataDirs.mapValues(s => Paths.get(s))),
        checkpointsDir
      )

    val resultTable = executeQuery(
      request.datasetID,
      inputSlices,
      typedMap(request.datasetVocabs),
      transform
    )

    println(s"Result schema:\n${resultTable.getSchema}")

    // Computes row count and data hash
    val resultStream = resultTable
      .toAppendStream[Row]
      .withStats(
        request.datasetID.toString,
        checkpointsDir.resolve(request.datasetID.toString)
      )
      .withDebugLogging(request.datasetID.toString)

    // Convert to Avro so we can then save in Parquet :(
    val avroSchema = SchemaConverter.convert(resultTable.getSchema)
    println(s"Result schema in Avro format:\n${avroSchema.toString(true)}")

    val avroConverter = new AvroConverter(avroSchema.toString())
    val avroStream =
      resultStream.map(r => avroConverter.convertRowToAvroRecord(r))

    val dataFilePath = Paths
      .get(request.dataDirs(request.datasetID.toString))
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
      Paths.get(request.checkpointsDir)
    )

    val stats =
      gatherStats(request.datasetID :: inputSlices.keys.toList, checkpointsDir)

    val block = MetadataBlock(
      prevBlockHash = "",
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = stats(request.datasetID).hash,
          interval = Interval.point(systemClock.instant()),
          numRecords = stats(request.datasetID).numRecords
        )
      ),
      outputWatermark = stats(request.datasetID).lastWatermark,
      inputSlices = request.source.inputs.map(
        i =>
          DataSlice(
            hash = stats(i.id).hash,
            interval = inputSlices(i.id).interval,
            numRecords = stats(i.id).numRecords
          )
      )
    )

    ExecuteQueryResult(
      block = block,
      dataFileName = Some(File(dataFilePath)).filter(_.exists).map(_.name)
    )
  }

  private def executeQuery(
    datasetID: DatasetID,
    inputSlices: Map[DatasetID, InputSlice],
    datasetVocabs: Map[DatasetID, DatasetVocabulary],
    transform: TransformKind.Flink
  ): Table = {
    val temporalTables = transform.temporalTables.map(t => (t.id, t)).toMap

    // Setup inputs
    for ((inputID, slice) <- inputSlices) {
      val inputVocab = datasetVocabs(inputID).withDefaults()

      val eventTimeColumn = inputVocab.eventTimeColumn.get

      val columns = FieldInfoUtils
        .getFieldNames(slice.dataStream.dataType)
        .map({
          case `eventTimeColumn` => s"$eventTimeColumn.rowtime"
          case other             => other
        })

      val expressions =
        ExpressionParser.parseExpressionList(columns.mkString(", ")).asScala

      val table = tEnv
        .fromDataStream(slice.dataStream, expressions: _*)
        .dropColumns(inputVocab.systemTimeColumn.get)

      logger.info(
        "Registered input {} with schema:\n{}",
        inputID,
        table.getSchema
      )

      tEnv.createTemporaryView(s"`$inputID`", table)

      temporalTables
        .get(inputID)
        .map(_.primaryKey)
        .getOrElse(Vector.empty) match {
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
      inputSlices.values.forall(i => File(i.markerPath).exists)
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
      inputSlices.values.foreach(i => File(i.markerPath).delete(true))
    }
  }

  private def prepareInputSlices(
    inputSlices: Map[DatasetID, InputDataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary],
    inputDataDirs: Map[DatasetID, Path],
    checkpointsDir: Path
  ): Map[DatasetID, InputSlice] = {
    inputSlices.keys.toSeq
      .sortBy(_.toString)
      .map(id => {
        (
          id,
          prepareInputSlice(
            id,
            inputSlices(id),
            inputVocabs(id).withDefaults(),
            inputDataDirs(id),
            checkpointsDir
          )
        )
      })
      .toMap
  }

  private def prepareInputSlice(
    id: DatasetID,
    slice: InputDataSlice,
    vocab: DatasetVocabulary,
    dataDir: Path,
    checkpointsDir: Path
  ): InputSlice = {
    val markerPath = checkpointsDir.resolve(s"$id.marker")
    val statsPath = checkpointsDir.resolve(id.toString)

    val prevStats = readStats(statsPath)

    // TODO: use schema from metadata
    val stream =
      sliceData(
        openStream(
          id,
          dataDir,
          markerPath,
          prevStats.flatMap(_.lastWatermark),
          slice.explicitWatermarks
        ),
        slice.interval,
        vocab
      )

    val eventTimeColumn = vocab.eventTimeColumn.get
    val eventTimePos = FieldInfoUtils
      .getFieldNames(stream.dataType)
      .indexOf(eventTimeColumn)

    if (eventTimePos < 0)
      throw new Exception(
        s"Event time column not found: $eventTimeColumn"
      )

    // TODO: Support delayed watermarking
    //val streamWithWatermarks = stream.assignTimestampsAndWatermarks(
    //  BoundedOutOfOrderWatermark.forRow(
    //    _.getField(eventTimePos).asInstanceOf[Timestamp].getTime,
    //    Duration.Zero
    //  )
    //)

    val streamWithWatermarks = stream.transform(
      "Timestamps/Watermarks",
      new CustomWatermarksOperator[Row](
        new TimestampAssigner(
          _.getField(eventTimePos).asInstanceOf[Timestamp].getTime
        )
      )
    )(stream.dataType)

    val streamWithStats = streamWithWatermarks
      .withStats(id.toString, statsPath)
      .withDebugLogging(id.toString)

    InputSlice(
      dataStream = streamWithStats,
      interval = slice.interval,
      markerPath = markerPath
    )
  }

  private def sliceData(
    stream: DataStream[Row],
    interval: Interval[Instant],
    vocab: DatasetVocabulary
  ): DataStream[Row] = {
    val schema = stream.dataType.asInstanceOf[RowTypeInfo]
    val systemTimeColumn = schema.getFieldIndex(vocab.systemTimeColumn.get)

    val (min, max) = interval match {
      case Empty() => (Instant.MAX, Instant.MIN)
      case All()   => (Instant.MIN, Instant.MAX)
      case _ =>
        (
          interval.lowerBound match {
            case Unbound() => Instant.MIN
            case Open(x)   => x
            case Closed(x) => x.minusMillis(1)
            case _         => throw new RuntimeException("Unexpected")
          },
          interval.upperBound match {
            case Unbound() => Instant.MAX
            case Open(x)   => x
            case Closed(x) => x.plusMillis(1)
            case _         => throw new RuntimeException("Unexpected")
          }
        )
    }

    stream.filter(row => {
      val systemTime = row
        .getField(systemTimeColumn)
        .asInstanceOf[Timestamp]
        .toInstant
      systemTime.compareTo(min) > 0 && systemTime.compareTo(max) < 0
    })
  }

  private def typedMap[T](m: Map[String, T]): Map[DatasetID, T] = {
    m.map {
      case (id, value) => (DatasetID(id), value)
    }
  }

  private def gatherStats(
    datasetIDs: Seq[DatasetID],
    checkpointsDir: Path
  ): Map[DatasetID, SliceStats] = {
    datasetIDs
      .map(id => (id, checkpointsDir.resolve(id.toString)))
      .map { case (id, p) => (id, readStats(p)) }
      .filter(_._2.isDefined)
      .map { case (id, s) => (id, s.get) }
      .toMap
  }

  private def readStats(path: Path): Option[SliceStats] = {
    if (!File(path).exists)
      return None

    try {
      val reader = new Scanner(path)

      val sRowCount = reader.nextLine()
      val sLastWatermark = reader.nextLine()
      val hash = reader.nextLine()

      val lLastWatermark = sLastWatermark.toLong

      reader.close()

      Some(
        SliceStats(
          hash = hash,
          lastWatermark =
            if (lLastWatermark == Long.MinValue) None
            else Some(Instant.ofEpochMilli(lLastWatermark)),
          numRecords = sRowCount.toLong
        )
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
    markerPath: Path,
    prevWatermark: Option[Instant],
    explicitWatermarks: Vector[Watermark]
  ): DataStream[Row] = {
    // TODO: Ignoring schema evolution
    val schema = getSchemaFromFile(findFirstParquetFile(path).get)
    logger.debug(s"Using following schema:\n$schema")

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
    )(inputFormat.getProducedType)*/

    new DataStream[Row](
      createFileInput(
        inputFormat,
        inputFormat.getProducedType,
        datasetID.toString,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        1000,
        markerPath,
        prevWatermark,
        explicitWatermarks
      )
    )
  }

  private def getSchemaFromFile(path: Path): MessageType = {
    logger.debug(s"Loading schema from: $path")
    val file = HadoopInputFile.fromPath(
      new org.apache.hadoop.fs.Path(path.toUri),
      new org.apache.hadoop.conf.Configuration()
    )
    val reader = ParquetFileReader.open(file)
    val schema = reader.getFileMetaData.getSchema
    reader.close()
    schema
  }

  private def findFirstParquetFile(path: Path): Option[Path] = {
    File(path).glob("*.parquet").map(_.path).toSeq.headOption
  }

  // TODO: This env method is overridden to customize file reader behavior
  private def createFileInput[T](
    inputFormat: FileInputFormat[T],
    typeInfo: TypeInformation[T],
    sourceName: String,
    monitoringMode: FileProcessingMode,
    interval: Long,
    markerPath: Path,
    prevWatermark: Option[Instant],
    explicitWatermarks: Vector[Watermark]
  ): DataStreamSource[T] = {
    val monitoringFunction =
      new CustomFileMonitoringFunction[T](
        inputFormat,
        monitoringMode,
        env.getParallelism,
        interval
      )

    val factory =
      new CustomFileReaderOperatorFactory[T, TimestampedFileInputSplit](
        inputFormat,
        markerPath.toUri.getPath,
        prevWatermark.getOrElse(Instant.MIN),
        explicitWatermarks.map(_.eventTime).asJava
      )

    val source = env.getJavaEnv
      .addSource(monitoringFunction, sourceName.asInstanceOf[String])
      .transform("Split Reader: " + sourceName, typeInfo, factory)

    new DataStreamSource(source)
  }

  implicit class StreamHelpers(s: DataStream[Row]) {

    def withStats(id: String, path: Path): DataStream[Row] = {
      s.transform(
        "stats",
        new StatsOperator(id, path.toUri.getPath)
      )(s.dataType)
    }

    def withDebugLogging(id: String): DataStream[Row] = {
      s.transform(
        "snitch",
        new SnitchOperator(id)
      )(s.dataType)
    }

  }
}
