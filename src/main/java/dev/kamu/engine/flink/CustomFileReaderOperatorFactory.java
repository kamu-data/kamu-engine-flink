package dev.kamu.engine.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.TimestampedInputSplit;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import java.time.Instant;
import java.util.List;

/** Patches {ContinuousFileReaderOperatorFactory} to instantiate our reader class
  * Thanks Flink...
  */
public class CustomFileReaderOperatorFactory<OUT, T extends TimestampedInputSplit>
        extends AbstractStreamOperatorFactory<OUT>
        implements YieldingOperatorFactory<OUT>, OneInputStreamOperatorFactory<T, OUT> {

    private final InputFormat<OUT, ? super T> inputFormat;
    private TypeInformation<OUT> type;
    private ExecutionConfig executionConfig;
    private transient MailboxExecutor mailboxExecutor;

    private String markerPath;
    private Instant prevWatermark;
    private List<Instant> explicitWatermarks;

    public CustomFileReaderOperatorFactory(
            InputFormat<OUT, ? super T> inputFormat,
            String markerPath,
            Instant prevWatermark,
            List<Instant> explicitWatermarks) {
        this(inputFormat, null, null, markerPath, prevWatermark, explicitWatermarks);
    }

    public CustomFileReaderOperatorFactory(
            InputFormat<OUT, ? super T> inputFormat, TypeInformation<OUT> type,
            ExecutionConfig executionConfig,
            String markerPath,
            Instant prevWatermark,
            List<Instant> explicitWatermarks) {
        this.inputFormat = inputFormat;
        this.type = type;
        this.executionConfig = executionConfig;
        this.chainingStrategy = ChainingStrategy.HEAD;

        this.markerPath = markerPath;
        this.prevWatermark = prevWatermark;
        this.explicitWatermarks = explicitWatermarks;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public <O extends StreamOperator<OUT>> O createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        CustomFileReaderOperator<OUT, T> operator = new CustomFileReaderOperator<>(
                inputFormat, processingTimeService, mailboxExecutor, markerPath, prevWatermark, explicitWatermarks);
        operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        operator.setOutputType(type, executionConfig);
        return (O) operator;
    }

    @Override
    public void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
        this.type = type;
        this.executionConfig = executionConfig;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CustomFileReaderOperator.class;
    }

    @Override
    public boolean isOutputTypeConfigurable() {
        return true;
    }
}
