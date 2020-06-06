package dev.kamu.engine.flink;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;

public class CustomLocalStreamExecutionEnvironment extends LocalStreamEnvironment {

    public JobClient executeFromSavepointAsync(String savepointPath) throws Exception {
        return executeAsync(getStreamGraph(DEFAULT_JOB_NAME, savepointPath, true));
    }

    public StreamGraph getStreamGraph(String jobName, String savepointPath, boolean clearTransformations) {
        StreamGraphGenerator gen = getStreamGraphGenerator();
        gen.setJobName(jobName);
        gen.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));
        StreamGraph streamGraph = gen.generate();
        if (clearTransformations) {
            this.transformations.clear();
        }
        return streamGraph;
    }

    private StreamGraphGenerator getStreamGraphGenerator() {
        if (transformations.size() <= 0) {
            throw new IllegalStateException("No operators defined in streaming topology. Cannot execute.");
        }
        return new StreamGraphGenerator(transformations, getConfig(), getCheckpointConfig())
                .setStateBackend(getStateBackend())
                .setChaining(isChainingEnabled)
                .setUserArtifacts(cacheFile)
                .setTimeCharacteristic(getStreamTimeCharacteristic())
                .setDefaultBufferTimeout(getBufferTimeout());
    }
}
