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

package dev.kamu.engine.flink;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Patches TimestampsAndPunctuatedWatermarksOperator to:
 * - propagate upstream watermarks
 */
public class CustomWatermarksOperator<T>
    extends AbstractUdfStreamOperator<T, AssignerWithPunctuatedWatermarks<T>>
    implements OneInputStreamOperator<T, T> {

    private static final long serialVersionUID = 1L;

    private long currentWatermark = Long.MIN_VALUE;

    public CustomWatermarksOperator(AssignerWithPunctuatedWatermarks<T> assigner) {
        super(assigner);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    private void maybeEmitWatermark(Watermark nextWatermark) {
        if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermark) {
            currentWatermark = nextWatermark.getTimestamp();
            output.emitWatermark(nextWatermark);
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        final T value = element.getValue();
        final long newTimestamp = userFunction.extractTimestamp(value,
                element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

        output.collect(element.replace(element.getValue(), newTimestamp));

        final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);
        maybeEmitWatermark(nextWatermark);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from
     * upstream (we rely only on the {@link AssignerWithPunctuatedWatermarks} to emit
     * watermarks from here).
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        maybeEmitWatermark(mark);
    }
}
