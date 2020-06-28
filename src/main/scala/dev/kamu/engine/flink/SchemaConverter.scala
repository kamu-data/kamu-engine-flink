package dev.kamu.engine.flink

import java.util

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.{AtomicDataType, DataType}

import scala.collection.JavaConverters._
import scala.util.Random

object SchemaConverter {
  def convert(tableSchema: TableSchema): Schema = {
    convertRow("Row", tableSchema)
  }

  protected def convertRow(
    name: String,
    tableSchema: TableSchema
  ): Schema = {
    val builder = SchemaBuilder.record(name).fields()

    val newBuilder =
      tableSchema.getTableColumns.asScala.foldLeft(builder)((b, col) => {
        convertField(b.name(col.getName), col.getType)
      })

    newBuilder.endRecord()
  }

  protected def convertField(
    b: SchemaBuilder.FieldBuilder[Schema],
    dataType: DataType
  ): SchemaBuilder.FieldAssembler[Schema] = {
    dataType match {
      case atomic: AtomicDataType =>
        atomic.getLogicalType match {
          case _: IntType =>
            b.`type`().nullable().intType().noDefault()
          case _: BigIntType =>
            b.`type`().nullable().longType().noDefault()
          case _: FloatType =>
            b.`type`().nullable().floatType().noDefault()
          case _: DoubleType =>
            b.`type`().nullable().doubleType().noDefault()
          case _: VarBinaryType =>
            b.`type`().nullable().bytesType().noDefault()
          case _: CharType =>
            b.`type`().nullable().stringType().noDefault()
          case _: VarCharType =>
            b.`type`().nullable().stringType().noDefault()
          case _: TimestampType =>
            b.`type`(nullable(timestampMillisSchema())).noDefault()
          case t: DecimalType =>
            b.`type`(nullable(decimalSchema(t.getPrecision, t.getScale)))
              .noDefault()
          case l: LegacyTypeInformationType[_] =>
            val typeClass = l.getTypeInformation.getTypeClass
            if (typeClass == classOf[java.math.BigDecimal]) {
              b.`type`(nullable(decimalSchema(38, 18))).noDefault()
            } else {
              throw new NotImplementedError(
                s"Unsupported legacy type: $typeClass"
              )
            }
          case logical =>
            throw new NotImplementedError(s"Unsupported logical type: $logical")
        }
      case _ =>
        throw new NotImplementedError(s"Unsupported type: $dataType")
    }
  }

  private def timestampMillisSchema(): Schema = {
    LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  }

  private def decimalSchema(precision: Int, scale: Int): Schema = {
    val size =
      ParquetSchemaConverter.computeMinBytesForDecimalPrecision(precision)

    LogicalTypes
      .decimal(precision, scale)
      .addToSchema(
        Schema
          .createFixed(randomAlpha.take(10).mkString(""), "", "", size)
      )
  }

  private def nullable(schema: Schema): Schema = {
    Schema.createUnion(
      util.Arrays.asList(
        schema,
        Schema.create(Schema.Type.NULL)
      )
    )
  }

  def randomAlpha: Stream[Char] = {
    def nextAlpha: Char = {
      val chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
      chars charAt (Random.nextInt(chars.length))
    }

    Stream.continually(nextAlpha)
  }
}
