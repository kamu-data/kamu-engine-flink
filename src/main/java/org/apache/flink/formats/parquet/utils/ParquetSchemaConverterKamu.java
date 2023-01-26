package org.apache.flink.formats.parquet.utils;

import org.apache.flink.table.types.logical.*;
import org.apache.parquet.schema.*;

// KAMU: Adding support for converting generic parquet schema into Flink's RowType
public class ParquetSchemaConverterKamu extends ParquetSchemaConverter {

    // TODO: Flink only supports TIMESTAMP(3) in places like event/row times and when converting to Avro
    static final int TIME_PRECISION = 3;

    public static RowType convertToRowType(MessageType messageType) {
        GroupType groupType = messageType.asGroupType();

        String[] fieldNames = new String[groupType.getFieldCount()];
        for (int i = 0; i != groupType.getFieldCount(); i++) {
            fieldNames[i] = groupType.getFieldName(i);
        }

        LogicalType[] fieldTypes = new LogicalType[groupType.getFieldCount()];
        for (int i = 0; i != groupType.getFieldCount(); i++) {
            fieldTypes[i] = convertParquetTypeToLogicalType(groupType.getType(i));
        }

        return RowType.of(fieldTypes, fieldNames);
    }

    public static LogicalType convertParquetTypeToLogicalType(final Type fieldType) {
        LogicalType logicalType;
        if (fieldType.isPrimitive()) {
            boolean isOptional = fieldType.getRepetition() == Type.Repetition.OPTIONAL;
            OriginalType originalType = fieldType.getOriginalType();
            PrimitiveType primitiveType = fieldType.asPrimitiveType();
            switch (primitiveType.getPrimitiveTypeName()) {
                case BINARY:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL:
                                DecimalMetadata meta = primitiveType.getDecimalMetadata();
                                logicalType = new DecimalType(isOptional, meta.getPrecision(), meta.getScale());
                                break;
                            case UTF8:
                            case ENUM:
                            case JSON:
                            case BSON:
                                logicalType = new VarCharType(isOptional, VarCharType.MAX_LENGTH);
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type BINARY");
                        }
                    } else {
                        logicalType = new VarCharType(isOptional, VarCharType.MAX_LENGTH);
                    }
                    break;
                case BOOLEAN:
                    logicalType = new BooleanType(isOptional);
                    break;
                case INT32:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL:
                                DecimalMetadata meta = primitiveType.getDecimalMetadata();
                                logicalType = new DecimalType(isOptional, meta.getPrecision(), meta.getScale());
                                break;
                            case TIME_MICROS:
                            case TIME_MILLIS:
                                logicalType = new TimeType(isOptional, TIME_PRECISION);
                                break;
                            case TIMESTAMP_MICROS:
                            case TIMESTAMP_MILLIS:
                                logicalType = new TimestampType(isOptional, TIME_PRECISION);
                                break;
                            case DATE:
                                logicalType = new DateType(isOptional);
                                break;
                            case UINT_8:
                            case UINT_16:
                            case UINT_32:
                                logicalType = new IntType(isOptional);
                                break;
                            case INT_8:
                                logicalType = new TinyIntType(isOptional);
                                break;
                            case INT_16:
                                logicalType = new SmallIntType(isOptional);
                                break;
                            case INT_32:
                                logicalType = new IntType(isOptional);
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type INT32");
                        }
                    } else {
                        logicalType = new IntType(isOptional);
                    }
                    break;
                case INT64:
                    if (originalType != null) {
                        switch (originalType) {
                            case TIME_MICROS:
                                logicalType = new TimeType(isOptional, TIME_PRECISION);
                                break;
                            case TIMESTAMP_MICROS:
                            case TIMESTAMP_MILLIS:
                                logicalType = new TimestampType(isOptional, TIME_PRECISION);
                                break;
                            case INT_64:
                                logicalType = new BigIntType(isOptional);
                                break;
                            case DECIMAL:
                                DecimalMetadata meta = primitiveType.getDecimalMetadata();
                                logicalType = new DecimalType(isOptional, meta.getPrecision(), meta.getScale());
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type INT64");
                        }
                    } else {
                        logicalType = new BigIntType(isOptional);
                    }
                    break;
                case INT96:
                    // It stores a timestamp type data, we read it as millisecond
                    logicalType = new TimestampType(isOptional, TIME_PRECISION);
                    break;
                case FLOAT:
                    logicalType = new FloatType(isOptional);
                    break;
                case DOUBLE:
                    logicalType = new DoubleType(isOptional);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL:
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type FIXED_LEN_BYTE_ARRAY");
                        }
                    } else {
                        throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
                    }
                    //break;
                default:
                    throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
            }
        } else {
            GroupType parquetGroupType = fieldType.asGroupType();
            OriginalType originalType = parquetGroupType.getOriginalType();
            if (originalType != null) {
                switch (originalType) {
                    case LIST:
                        if (parquetGroupType.getFieldCount() != 1) {
                            throw new UnsupportedOperationException(
                                    "Invalid list type " + parquetGroupType);
                        }
                        Type repeatedType = parquetGroupType.getType(0);
                        if (!repeatedType.isRepetition(Type.Repetition.REPEATED)) {
                            throw new UnsupportedOperationException(
                                    "Invalid list type " + parquetGroupType);
                        }

                        if (repeatedType.isPrimitive()) {
                            logicalType = convertParquetPrimitiveListToFlinkArray(repeatedType);
                        } else {
                            // Backward-compatibility element group name can be any string
                            // (element/array/other)
                            GroupType elementType = repeatedType.asGroupType();
                            // If the repeated field is a group with multiple fields, then its type
                            // is the element
                            // type and elements are required.
                            if (elementType.getFieldCount() > 1) {
                                logicalType =
                                        convertGroupElementToArrayTypeInfo(
                                                parquetGroupType, elementType);
                            } else {
                                Type internalType = elementType.getType(0);
                                if (internalType.isPrimitive()) {
                                    logicalType =
                                            convertParquetPrimitiveListToFlinkArray(internalType);
                                } else {
                                    // No need to do special process for group named array and tuple
                                    GroupType tupleGroup = internalType.asGroupType();
                                    if (tupleGroup.getFieldCount() == 1
                                            && tupleGroup
                                            .getFields()
                                            .get(0)
                                            .isRepetition(Type.Repetition.REQUIRED)) {
                                        logicalType = new ArrayType(convertParquetTypeToLogicalType(internalType));
                                    } else {
                                        logicalType =
                                                convertGroupElementToArrayTypeInfo(
                                                        parquetGroupType, tupleGroup);
                                    }
                                }
                            }
                        }
                        break;

                    case MAP_KEY_VALUE:
                    case MAP:
                        // The outer-most level must be a group annotated with MAP
                        // that contains a single field named key_value
                        if (parquetGroupType.getFieldCount() != 1
                                || parquetGroupType.getType(0).isPrimitive()) {
                            throw new UnsupportedOperationException(
                                    "Invalid map type " + parquetGroupType);
                        }

                        // The middle level  must be a repeated group with a key field for map keys
                        // and, optionally, a value field for map values. But we can't enforce two
                        // strict condition here
                        // the schema generated by Parquet lib doesn't contain LogicalType
                        // ! mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE)
                        GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
                        if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED)
                                || mapKeyValType.getFieldCount() != 2) {
                            throw new UnsupportedOperationException(
                                    "The middle level of Map should be single field named key_value. Invalid map type "
                                            + parquetGroupType);
                        }

                        Type keyType = mapKeyValType.getType(0);

                        // The key field encodes the map's key type. This field must have repetition
                        // required and
                        // must always be present.
                        if (!keyType.isPrimitive()
                                || !keyType.isRepetition(Type.Repetition.REQUIRED)
                                || !keyType.asPrimitiveType()
                                .getPrimitiveTypeName()
                                .equals(PrimitiveType.PrimitiveTypeName.BINARY)
                                || !keyType.getOriginalType().equals(OriginalType.UTF8)) {
                            throw new IllegalArgumentException(
                                    "Map key type must be required binary (UTF8): " + keyType);
                        }

                        Type valueType = mapKeyValType.getType(1);
                        return new MapType(
                                VarCharType.STRING_TYPE,
                                convertParquetTypeToLogicalType(valueType));
                    default:
                        throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
                }
            } else {
                // if no original type than it is a record
                throw new UnsupportedOperationException("Record types are not implemented yet: " + parquetGroupType);
                //return convertFields(parquetGroupType.getFields());
            }
        }

        return logicalType;
    }

    private static ArrayType convertGroupElementToArrayTypeInfo(
            GroupType arrayFieldType, GroupType elementType) {
        for (Type type : elementType.getFields()) {
            if (!type.isRepetition(Type.Repetition.REQUIRED)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "List field [%s] in List [%s] has to be required. ",
                                type.toString(), arrayFieldType.getName()));
            }
        }
        return new ArrayType(convertParquetTypeToLogicalType(elementType));
    }

    private static LogicalType convertParquetPrimitiveListToFlinkArray(Type type) {
        throw new UnsupportedOperationException("Unsupported type" + type);
        /*// Backward-compatibility element group doesn't exist also allowed
        LogicalType flinkType = convertParquetTypeToLogicalType(type);
        if (flinkType.isBasicType()) {
            return BasicArrayTypeInfo.getInfoFor(
                    Array.newInstance(flinkType.getTypeClass(), 0).getClass());
        } else {
            // flinkType here can be either SqlTimeTypeInfo or BasicTypeInfo.BIG_DEC_TYPE_INFO,
            // So it should be converted to ObjectArrayTypeInfo
            return ObjectArrayTypeInfo.getInfoFor(flinkType);
        }*/
    }
}
