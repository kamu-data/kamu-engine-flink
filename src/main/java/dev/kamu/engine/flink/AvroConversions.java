package dev.kamu.engine.flink;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class AvroConversions {

    public static class LocalDateTimeConversion extends Conversion<LocalDateTime> {
        @Override
        public Class<LocalDateTime> getConvertedType() {
            return LocalDateTime.class;
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        @Override
        public Long toLong(LocalDateTime value, Schema schema, LogicalType type) {
            return value.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
    }

}