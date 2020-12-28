package dev.kamu.engine.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.RowMaterializer;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A copy of RowReadSupport for sole purpose of overriding RowMaterializer
 */
public class RowReadSupportEx extends ReadSupport<Row> {

    private TypeInformation<?> returnTypeInfo;

    @Override
    public ReadContext init(InitContext initContext) {
        checkNotNull(initContext, "initContext");
        returnTypeInfo = ParquetSchemaConverter.fromParquetType(initContext.getFileSchema());
        return new ReadContext(initContext.getFileSchema());
    }

    @Override
    public RecordMaterializer<Row> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new RowMaterializerEx(readContext.getRequestedSchema(), returnTypeInfo);
    }
}