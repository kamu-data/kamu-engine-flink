package dev.kamu.engine.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Copied from RowMaterializer for sole purpose of replacing RowConverter
 */
public class RowMaterializerEx extends RecordMaterializer<Row> {
    private RowConverterEx root;

    public RowMaterializerEx(MessageType messageType, TypeInformation<?> rowTypeInfo) {
        checkNotNull(messageType, "messageType");
        checkNotNull(rowTypeInfo, "rowTypeInfo");
        this.root = new RowConverterEx(messageType, rowTypeInfo);
    }

    @Override
    public Row getCurrentRecord() {
        return root.getCurrentRow();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}