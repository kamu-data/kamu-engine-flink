package dev.kamu.engine.flink;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/** Customizes {@link ContinuousFileMonitoringFunction} from Flink
 * - Sorts files by name instead of modification time
 * - Signals reader that the last available split was read
 * */
public class CustomFileMonitoringFunction<OUT>
	extends RichSourceFunction<TimestampedFileInputSplit> implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CustomFileMonitoringFunction.class);

	/**
	 * The minimum interval allowed between consecutive path scans.
	 *
	 * <p><b>NOTE:</b> Only applicable to the {@code PROCESS_CONTINUOUSLY} mode.
	 */
	public static final long MIN_MONITORING_INTERVAL = 1L;

	/** The path to monitor. */
	private final String path;

	/** The parallelism of the downstream readers. */
	private final int readerParallelism;

	/** The {@link FileInputFormat} to be read. */
	private final FileInputFormat<OUT> format;

	/** The interval between consecutive path scans. */
	private final long interval;

	/** Which new data to process (see {@link FileProcessingMode}. */
	private final FileProcessingMode watchType;

    /** Last seen file name is kept as a state to determine which files were seen before */
    private volatile String lastSeenFilename = "";

	private transient Object checkpointLock;

	private volatile boolean isRunning = true;

    private transient ListState<String> checkpointedState;

    public CustomFileMonitoringFunction(
		FileInputFormat<OUT> format,
		FileProcessingMode watchType,
		int readerParallelism,
		long interval) {

		Preconditions.checkArgument(
			watchType == FileProcessingMode.PROCESS_ONCE || interval >= MIN_MONITORING_INTERVAL,
			"The specified monitoring interval (" + interval + " ms) is smaller than the minimum " +
				"allowed one (" + MIN_MONITORING_INTERVAL + " ms)."
		);

		Preconditions.checkArgument(
			format.getFilePaths().length == 1,
			"FileInputFormats with multiple paths are not supported yet.");

		this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
		this.path = Preconditions.checkNotNull(format.getFilePaths()[0].toString(), "Unspecified Path.");

		this.interval = interval;
		this.watchType = watchType;
		this.readerParallelism = Math.max(readerParallelism, 1);
        this.lastSeenFilename = "";
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		this.checkpointedState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"file-monitoring-state",
                        StringSerializer.INSTANCE
			)
		);

		if (context.isRestored()) {
			LOG.info("Restoring state for the {}.", getClass().getSimpleName());

            List<String> retrievedStates = new ArrayList<>();
            for (String entry : this.checkpointedState.get()) {
				retrievedStates.add(entry);
			}

			// given that the parallelism of the function is 1, we can only have 1 or 0 retrieved items.
			// the 0 is for the case that we are migrating from a previous Flink version.

			Preconditions.checkArgument(retrievedStates.size() <= 1,
				getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1 && !lastSeenFilename.isEmpty()) {
				// this is the case where we have both legacy and new state.
				// The two should be mutually exclusive for the operator, thus we throw the exception.

				throw new IllegalArgumentException(
					"The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

			} else if (retrievedStates.size() == 1) {
                this.lastSeenFilename = retrievedStates.get(0);
				if (LOG.isDebugEnabled()) {
                    LOG.debug("{} retrieved a last filename of {}.",
                            getClass().getSimpleName(), lastSeenFilename);
				}
			}

		} else {
			LOG.info("No state to restore for the {}.", getClass().getSimpleName());
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		format.configure(parameters);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opened {} (taskIdx= {}) for path: {}",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), path);
		}
	}

	@Override
	public void run(SourceFunction.SourceContext<TimestampedFileInputSplit> context) throws Exception {
		Path p = new Path(path);
		FileSystem fileSystem = FileSystem.get(p.toUri());
		if (!fileSystem.exists(p)) {
			throw new FileNotFoundException("The provided file path " + path + " does not exist.");
		}

		checkpointLock = context.getCheckpointLock();
		switch (watchType) {
			case PROCESS_CONTINUOUSLY:
				if (isRunning) {
					synchronized (checkpointLock) {
						monitorDirAndForwardSplits(fileSystem, context);
					}
				}

				context.collect(new TimestampedFileInputSplit(Long.MIN_VALUE, -1, null, 0, 0, null));

				while (isRunning) {
					Thread.sleep(interval);
				}

				// here we do not need to set the running to false and the
				// globalModificationTime to Long.MAX_VALUE because to arrive here,
				// either close() or cancel() have already been called, so this
				// is already done.

				break;
			case PROCESS_ONCE:
				synchronized (checkpointLock) {

					// the following check guarantees that if we restart
					// after a failure and we managed to have a successful
					// checkpoint, we will not reprocess the directory.

                    if (lastSeenFilename.isEmpty()) {
						monitorDirAndForwardSplits(fileSystem, context);
					}
					isRunning = false;
				}
				break;
			default:
				isRunning = false;
				throw new RuntimeException("Unknown WatchType" + watchType);
		}
	}

	private void monitorDirAndForwardSplits(FileSystem fs,
											SourceContext<TimestampedFileInputSplit> context) throws IOException {
		assert (Thread.holdsLock(checkpointLock));

        List<FileStatus> eligibleFiles = listEligibleFiles(fs, new Path(path));
        Map<String, List<TimestampedFileInputSplit>> splitsGrouped = getInputSplits(eligibleFiles);

        for (Map.Entry<String, List<TimestampedFileInputSplit>> splits: splitsGrouped.entrySet()) {
            String name = splits.getKey();
			for (TimestampedFileInputSplit split: splits.getValue()) {
				LOG.info("Forwarding split: " + split);
				context.collect(split);
			}

            lastSeenFilename = name;
		}
	}

	/**
	 * Creates the input splits to be forwarded to the downstream tasks of the
	 * {@link ContinuousFileReaderOperator}. Splits are sorted <b>by modification time</b> before
	 * being forwarded and only splits belonging to files in the {@code eligibleFiles}
	 * list will be processed.
	 * @param eligibleFiles The files to process.
	 */
    private Map<String, List<TimestampedFileInputSplit>> getInputSplits(List<FileStatus> eligibleFiles) throws IOException {
        Map<Path, FileStatus> eligibleFilesMap = new HashMap<>();
        for(FileStatus fs: eligibleFiles) {
            eligibleFilesMap.put(fs.getPath(), fs);
        }

        Map<String, List<TimestampedFileInputSplit>> splitsByName = new TreeMap<>();
		if (eligibleFiles.isEmpty()) {
            return splitsByName;
		}

		for (FileInputSplit split: format.createInputSplits(readerParallelism)) {
            FileStatus fileStatus = eligibleFilesMap.get(split.getPath());
			if (fileStatus != null) {
                String name = fileStatus.getPath().getName();
                long modTime = fileStatus.getModificationTime();
                List<TimestampedFileInputSplit> splitsToForward = splitsByName.get(name);
				if (splitsToForward == null) {
					splitsToForward = new ArrayList<>();
                    splitsByName.put(name, splitsToForward);
				}
				splitsToForward.add(new TimestampedFileInputSplit(
					modTime, split.getSplitNumber(), split.getPath(),
					split.getStart(), split.getLength(), split.getHostnames()));
			}
		}
        return splitsByName;
	}

	/**
	 * Returns the paths of the files not yet processed.
	 * @param fileSystem The filesystem where the monitored directory resides.
	 */
    private List<FileStatus> listEligibleFiles(FileSystem fileSystem, Path path) throws IOException {
        ArrayList<FileStatus> statuses;
		try {
            statuses = new ArrayList<>(Arrays.asList(fileSystem.listStatus(path)));
		} catch (IOException e) {
			// we may run into an IOException if files are moved while listing their status
			// delay the check for eligible files in this case
            return Collections.emptyList();
		}

        statuses.removeIf(fs ->
                !lastSeenFilename.isEmpty() &&
                fs.getPath().getName().compareToIgnoreCase(lastSeenFilename) <= 0);

        statuses.sort((lhs, rhs) -> lhs.getPath().getName().compareToIgnoreCase(rhs.getPath().getName()));
        return statuses;
		}

	@Override
	public void close() throws Exception {
		super.close();

		if (checkpointLock != null) {
			synchronized (checkpointLock) {
                //globalModificationTime = Long.MAX_VALUE;
				isRunning = false;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Closed File Monitoring Source for path: " + path + ".");
		}
	}

	@Override
	public void cancel() {
		if (checkpointLock != null) {
			// this is to cover the case where cancel() is called before the run()
			synchronized (checkpointLock) {
                //globalModificationTime = Long.MAX_VALUE;
				isRunning = false;
			}
		} else {
            //globalModificationTime = Long.MAX_VALUE;
			isRunning = false;
		}
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		this.checkpointedState.clear();
        this.checkpointedState.add(this.lastSeenFilename);

		if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpointed {}.", getClass().getSimpleName(), lastSeenFilename);
		}
	}
}
