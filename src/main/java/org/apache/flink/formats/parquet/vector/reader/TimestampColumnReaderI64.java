/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.writable.WritableIntVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableTimestampVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class TimestampColumnReaderI64 extends AbstractColumnReaderKamu<WritableTimestampVector> {

    public static final long MICROS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
    public static final long NANOS_PER_MICROSECONDS = TimeUnit.MICROSECONDS.toNanos(1);
    public static final long MILLIS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    public static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    public static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    public static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private final boolean utcTimestamp;

    private final LogicalTypeAnnotation.TimeUnit timeUnit;

    public TimestampColumnReaderI64(
            boolean utcTimestamp, ColumnDescriptor descriptor, PageReader pageReader)
            throws IOException {
        super(descriptor, pageReader);
        this.utcTimestamp = utcTimestamp;
        checkTypeName(PrimitiveType.PrimitiveTypeName.INT64);
        timeUnit =
                ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                        descriptor.getPrimitiveType().getLogicalTypeAnnotation())
                        .getUnit();
    }

    @Override
    protected boolean supportLazyDecode() {
        return utcTimestamp;
    }

    @Override
    protected void readBatch(int rowId, int num, WritableTimestampVector column) {
        for (int i = 0; i < num; i++) {
            if (runLenDecoder.readInteger() == maxDefLevel) {
                ByteBuffer buffer = readDataBuffer(8);
                column.setTimestamp(
                        rowId + i, int64ToTimestamp(utcTimestamp, buffer.getLong(), timeUnit));
            } else {
                column.setNullAt(rowId + i);
            }
        }
    }

    @Override
    protected void readBatchFromDictionaryIds(
            int rowId, int num, WritableTimestampVector column, WritableIntVector dictionaryIds) {
        for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
                column.setTimestamp(
                        i,
                        decodeInt64ToTimestamp(
                                utcTimestamp, dictionary, dictionaryIds.getInt(i), timeUnit));
            }
        }
    }

    public static TimestampData decodeInt64ToTimestamp(
            boolean utcTimestamp,
            org.apache.parquet.column.Dictionary dictionary,
            int id,
            LogicalTypeAnnotation.TimeUnit timeUnit) {
        return int64ToTimestamp(utcTimestamp, dictionary.decodeToLong(id), timeUnit);
    }

    public static TimestampData int64ToTimestamp(
            boolean utcTimestamp, long value, LogicalTypeAnnotation.TimeUnit timeUnit) {
        long nanosOfMillisecond = 0L;
        long milliseconds = 0L;

        switch (timeUnit) {
            case MILLIS:
                milliseconds = value;
                nanosOfMillisecond = value % MILLIS_PER_SECOND * NANOS_PER_MILLISECOND;
                break;
            case MICROS:
                milliseconds = value / MICROS_PER_MILLISECOND;
                nanosOfMillisecond = (value % MICROS_PER_SECOND) * NANOS_PER_MICROSECONDS;
                break;
            case NANOS:
                milliseconds = value / NANOS_PER_MILLISECOND;
                nanosOfMillisecond = value % NANOS_PER_SECOND;
                break;
            default:
                break;
        }

        if (utcTimestamp) {
            return TimestampData.fromEpochMillis(
                    milliseconds, (int) (nanosOfMillisecond % NANOS_PER_MILLISECOND));
        }
        Timestamp timestamp = new Timestamp(milliseconds);
        timestamp.setNanos((int) nanosOfMillisecond);
        return TimestampData.fromTimestamp(timestamp);
    }
}
