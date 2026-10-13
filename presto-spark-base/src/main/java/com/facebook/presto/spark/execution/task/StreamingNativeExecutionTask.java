/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spark.execution.http.StreamingBatchTaskUpdateRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A {@link NativeExecutionTask} whose splits are streamed into the task update request instead of
 * being held in a list, so they are never all alive at once. Used only when
 * {@code native_execution_stream_task_sources_enabled} is set; see {@link StreamingBatchTaskUpdateRequest}.
 *
 * <p>Everything except how the update request is sent is inherited unchanged.
 */
public class StreamingNativeExecutionTask
        extends NativeExecutionTask
{
    private final TaskId taskId;
    private final Session session;
    private final PrestoSparkHttpTaskClient workerClient;
    private final PlanFragment planFragment;
    private final TableWriteInfo tableWriteInfo;
    private final Optional<String> shuffleWriteInfo;
    private final Optional<String> broadcastBasePath;
    private final OutputBuffers outputBuffers;
    private final JsonCodec<StreamingBatchTaskUpdateRequest> streamingTaskUpdateRequestCodec;
    // Single pass; cleared once the update request has been written.
    private Iterator<TaskSource> sourceStream;

    public StreamingNativeExecutionTask(
            TaskId taskId,
            Session session,
            PrestoSparkHttpTaskClient workerClient,
            PlanFragment planFragment,
            Iterator<TaskSource> sourceStream,
            TableWriteInfo tableWriteInfo,
            Optional<String> shuffleWriteInfo,
            Optional<String> broadcastBasePath,
            ScheduledExecutorService scheduledExecutorService,
            TaskManagerConfig taskManagerConfig,
            JsonCodec<StreamingBatchTaskUpdateRequest> streamingTaskUpdateRequestCodec)
    {
        // The base class holds no splits for this task: they are pulled from sourceStream while
        // the update request is written.
        super(taskId, session, workerClient, planFragment, ImmutableList.of(), tableWriteInfo, shuffleWriteInfo, broadcastBasePath, scheduledExecutorService, taskManagerConfig);
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.session = requireNonNull(session, "session is null");
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.sourceStream = requireNonNull(sourceStream, "sourceStream is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.shuffleWriteInfo = requireNonNull(shuffleWriteInfo, "shuffleWriteInfo is null");
        this.broadcastBasePath = requireNonNull(broadcastBasePath, "broadcastBasePath is null");
        this.streamingTaskUpdateRequestCodec = requireNonNull(streamingTaskUpdateRequestCodec, "streamingTaskUpdateRequestCodec is null");
        // Same output buffers NativeExecutionTask sends.
        this.outputBuffers = createInitialEmptyOutputBuffers(planFragment.getPartitioningScheme().getPartitioning().getHandle()).withNoMoreBufferIds();
    }

    @Override
    protected TaskInfo sendUpdateRequest()
    {
        checkState(sourceStream != null, "task %s already sent its update request", taskId);
        Iterator<TaskSource> sources = sourceStream;
        // Drop the stream: this task stays alive until the query finishes, and the drained
        // iterator would otherwise keep what it references reachable.
        sourceStream = null;
        return workerClient.updateTaskStreaming(
                taskId,
                sources,
                planFragment,
                tableWriteInfo,
                shuffleWriteInfo,
                broadcastBasePath,
                session,
                outputBuffers,
                streamingTaskUpdateRequestCodec);
    }
}
