package dev.kamu.engine.flink

import java.sql.Timestamp
import java.time.Instant

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.TransformTaskConfig
import dev.kamu.core.utils.Clock
import dev.kamu.core.utils.fs._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.{Path => FlinkPath}
import org.apache.flink.formats.parquet.ParquetRowInputFormat
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
import spire.math.{Empty, Interval}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

case class InputSlice(
  dataStream: DataStream[Row],
  dataSlice: DataSlice
)

class Engine(
  fileSystem: FileSystem,
  systemClock: Clock,
  env: StreamExecutionEnvironment,
  tEnv: StreamTableEnvironment
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def executeQueryExtended(task: TransformTaskConfig): Unit = {
    if (task.source.transformEngine != "flink")
      throw new RuntimeException(
        s"Invalid engine: ${task.source.transformEngine}"
      )

    val transform =
      yaml.load[TransformKind.Flink](task.source.transform.toConfig)

    val inputSlices =
      prepareInputSlices(
        typedMap(task.inputSlices),
        typedMap(task.datasetVocabs),
        typedMap(task.datasetLayouts)
      )

    val resultTable = executeQuery(task.datasetID, inputSlices, transform)

    logger.info("Result schema:\n{}", resultTable.getSchema)
    val resultStream = resultTable.toAppendStream[Row]

    // Convert to Avro so we can then save in Parquet :(
    val avroSchema = SchemaConverter.convert(resultTable.getSchema)
    logger.info("Result schema in Avro format:\n{}", avroSchema.toString(true))

    val avroConverter = new AvroConverter(avroSchema.toString())
    val avroStream =
      resultStream.map(r => avroConverter.convertRowToAvroRecord(r))

    avroStream.addSink(
      new ParuqetSink(
        avroSchema.toString(),
        task
          .datasetLayouts(task.datasetID.toString)
          .dataDir
          .resolve("part.snappy.parquet")
          .toString
      )
    )

    env.execute()

    // TODO: Compute hash, interval and num records
    val block = MetadataBlock(
      prevBlockHash = "",
      // TODO: Current time? Min of input times? Require to propagate in computations?
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = "XXXXXXXXXXXXXX",
          interval = Interval.point(systemClock.instant()),
          numRecords = 0
        )
      ),
      inputSlices = task.source.inputs.map(i => inputSlices(i.id).dataSlice)
    )

    val outputStream =
      fileSystem.create(task.metadataOutputDir.resolve("block.yaml"))
    yaml.save(Manifest(block), outputStream)
    outputStream.close()
  }

  def executeQuery(
    datasetID: DatasetID,
    inputSlices: Map[DatasetID, InputSlice],
    transform: TransformKind.Flink
  ): Table = {
    // Prepare watermarks
    val watermarks = transform.watermarks.map(w => (w.id, w)).toMap

    // Setup inputs
    for ((inputID, slice) <- inputSlices) {
      val watermark = watermarks.get(inputID)

      val event_time = watermark.map(_.eventTimeColumn).getOrElse("")

      val columns = FieldInfoUtils
        .getFieldNames(slice.dataStream.dataType)
        .map({
          case `event_time` => s"$event_time.rowtime"
          case other        => other
        })

      val expressions =
        ExpressionParser.parseExpressionList(columns.mkString(", ")).asScala

      val event_time_pos = FieldInfoUtils
        .getFieldNames(slice.dataStream.dataType)
        .indexOf(event_time)

      val stream = slice.dataStream.assignTimestampsAndWatermarks(
        BoundedOutOfOrderWatermark.forRow(
          _.getField(event_time_pos).asInstanceOf[Timestamp].getTime,
          watermark.flatMap(_.maxLateBy).getOrElse(Duration.Zero)
        )
      )

      val table = tEnv
        .fromDataStream(stream, expressions: _*)

      logger.info("Input {} schema:\n{}", inputID, table.getSchema)
      tEnv.createTemporaryView(s"`$inputID`", table)
    }

    // Setup transform
    for (step <- transform.queries) {
      tEnv.createTemporaryView(
        s"`${step.alias.getOrElse(datasetID.toString)}`",
        tEnv.sqlQuery(step.query)
      )
    }

    // Get result
    tEnv.sqlQuery(s"SELECT * FROM `$datasetID`")
  }

  private def prepareInputSlices(
    inputSlices: Map[DatasetID, DataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary],
    inputLayouts: Map[DatasetID, DatasetLayout]
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice =
          prepareInputSlice(id, slice, inputVocabs(id), inputLayouts(id))
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    id: DatasetID,
    slice: DataSlice,
    vocab: DatasetVocabulary,
    layout: DatasetLayout
  ): InputSlice = {
    // TODO: use schema from metadata
    val stream = sliceData(openStream(layout.dataDir), slice.interval, vocab)

    // TODO: Compute real hash and count rows
    InputSlice(
      dataStream = stream,
      dataSlice = slice.copy(hash = "XXXXXXXXXXXXX", numRecords = 0)
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
      case _ =>
        val schema = stream.dataType.asInstanceOf[RowTypeInfo]
        val systemTimeColumn = schema.getFieldIndex(vocab.systemTimeColumn)

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

  private def openStream(path: Path): DataStream[Row] = {
    // TODO: Ignoring schema evolution
    val schema = getSchemaFromFile(findFirstParquetFile(path).get)
    logger.debug("Using following schema:\n{}", schema)

    val messageType = MessageTypeParser.parseMessageType(schema.toString)

    env.readFile[Row](
      new ParquetRowInputFormat(
        new FlinkPath(path.toString),
        messageType
      ),
      path.toString
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
}
