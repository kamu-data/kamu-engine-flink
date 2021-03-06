package dev.kamu.engine.flink;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

public class AvroConverter implements Serializable {
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    private final String stringSchema;

    private transient Schema schema;

    public AvroConverter(String avroSchema) {
        stringSchema = avroSchema;
    }

    public GenericRecord convertRowToAvroRecord(Row row) {
        if(schema == null) {
            schema = new Schema.Parser().parse(stringSchema);
        }
        return convertRowToAvroRecord(schema, row);
    }

    private static GenericRecord convertRowToAvroRecord(Schema schema, Row row) {
        final List<Schema.Field> fields = schema.getFields();
        final int length = fields.size();
        final GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < length; i++) {
            final Schema.Field field = fields.get(i);
            record.put(i, convertFlinkType(field.schema(), row.getField(i)));
        }
        return record;
    }

    private static Object convertFlinkType(Schema schema, Object object) {
        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                if (object instanceof Row) {
                    return convertRowToAvroRecord(schema, (Row) object);
                }
                throw new IllegalStateException("Row expected but was: " + object.getClass());
            case ENUM:
                return new GenericData.EnumSymbol(schema, object.toString());
            case ARRAY:
                final Schema elementSchema = schema.getElementType();
                final Object[] array = (Object[]) object;
                final GenericData.Array<Object> convertedArray = new GenericData.Array<>(array.length, schema);
                for (Object element : array) {
                    convertedArray.add(convertFlinkType(elementSchema, element));
                }
                return convertedArray;
            case MAP:
                final Map<?, ?> map = (Map<?, ?>) object;
                final Map<Utf8, Object> convertedMap = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    convertedMap.put(
                            new Utf8(entry.getKey().toString()),
                            convertFlinkType(schema.getValueType(), entry.getValue()));
                }
                return convertedMap;
            case UNION:
                final List<Schema> types = schema.getTypes();
                final int size = types.size();
                final Schema actualSchema;
                if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    actualSchema = types.get(1);
                } else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                    actualSchema = types.get(0);
                } else if (size == 1) {
                    actualSchema = types.get(0);
                } else {
                    // generic type
                    return object;
                }
                return convertFlinkType(actualSchema, object);
            case FIXED:
                // check for logical type
                if (object instanceof BigDecimal) {
                    return new GenericData.Fixed(
                            schema,
                            convertFromDecimal(schema, (BigDecimal) object));
                }
                return new GenericData.Fixed(schema, (byte[]) object);
            case STRING:
                return new Utf8(object.toString());
            case BYTES:
                // check for logical type
                if (object instanceof BigDecimal) {
                    return ByteBuffer.wrap(convertFromDecimal(schema, (BigDecimal) object));
                }
                return ByteBuffer.wrap((byte[]) object);
            case INT:
                // check for logical types
                if (object instanceof Date) {
                    return convertFromDate(schema, (Date) object);
                } else if (object instanceof Time) {
                    return convertFromTime(schema, (Time) object);
                }
                return object;
            case LONG:
                // check for logical type
                if (object instanceof Timestamp) {
                    return convertFromTimestamp(schema, (Timestamp) object);
                }
                return object;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object;
        }
        throw new RuntimeException("Unsupported Avro type:" + schema);
    }

    private static byte[] convertFromDecimal(Schema schema, BigDecimal decimal) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
            final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            // rescale to target type
            final BigDecimal rescaled = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
            // byte array must contain the two's-complement representation of the
            // unscaled integer value in big-endian byte order
            byte[] decimalRepr = rescaled.unscaledValue().toByteArray();
            if(schema.getType() == Schema.Type.FIXED && schema.getFixedSize() != decimalRepr.length) {
                // Need to re-scale
                byte signByte = (byte) (decimalRepr[0] < 0 ? -1 : 0);
                byte[] padded = new byte[schema.getFixedSize()];
                int startAt = padded.length - decimalRepr.length;
                Arrays.fill(padded, 0, startAt, signByte);
                System.arraycopy(decimalRepr, 0, padded, startAt, decimalRepr.length);
                return padded;
            }
            return decimalRepr;
        } else {
            throw new RuntimeException("Unsupported decimal type.");
        }
    }

    private static int convertFromDate(Schema schema, Date date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.date()) {
            // adopted from Apache Calcite
            final long time = date.getTime();
            final long converted = time + (long) LOCAL_TZ.getOffset(time);
            return (int) (converted / 86400000L);
        } else {
            throw new RuntimeException("Unsupported date type.");
        }
    }

    private static int convertFromTime(Schema schema, Time date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.timeMillis()) {
            // adopted from Apache Calcite
            final long time = date.getTime();
            final long converted = time + (long) LOCAL_TZ.getOffset(time);
            return (int) (converted % 86400000L);
        } else {
            throw new RuntimeException("Unsupported time type.");
        }
    }

    private static long convertFromTimestamp(Schema schema, Timestamp date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.timestampMillis()) {
            // adopted from Apache Calcite
            final long time = date.getTime();
            return time + (long) LOCAL_TZ.getOffset(time);
        } else {
            throw new RuntimeException("Unsupported timestamp type.");
        }
    }
}
