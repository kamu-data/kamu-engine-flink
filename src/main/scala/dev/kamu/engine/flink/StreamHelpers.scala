package dev.kamu.engine.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.RowRowConverter
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.runtime.typeutils.{
  ExternalTypeInfo,
  InternalTypeInfo
}
import org.apache.flink.types.Row

object StreamHelpers {
  implicit class DataStreamRowDataExt(s: DataStream[RowData]) {
    def toExternal: DataStream[Row] = {
      val typeInfo = s.dataType.asInstanceOf[InternalTypeInfo[RowData]]
      val dataType = typeInfo.getDataType

      val newTypeInfo =
        TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
          dataType.bridgedTo(classOf[Row])
        )

      val converter = RowRowConverter.create(dataType)
      converter.open(this.getClass.getClassLoader)

      s.map(row => converter.toExternal(row))(
        newTypeInfo.asInstanceOf[TypeInformation[Row]]
      )
    }
  }

  implicit class DataStreamRowExt(s: DataStream[Row]) {
    def toInternal: DataStream[RowData] = {
      val typeInfo = s.dataType.asInstanceOf[ExternalTypeInfo[Row]]
      val dataType = typeInfo.getDataType

      val converter = RowRowConverter.create(dataType)
      converter.open(this.getClass.getClassLoader)

      s.map(row => converter.toInternal(row))(
        InternalTypeInfo.of(dataType.getLogicalType)
      )
    }
  }
}
