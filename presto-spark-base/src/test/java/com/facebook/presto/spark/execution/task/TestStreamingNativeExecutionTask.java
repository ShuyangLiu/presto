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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spark.PrestoSparkSessionProperties;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.StreamingBatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingOkHttpClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingResponseManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import okhttp3.Request;
import okhttp3.Response;
import okio.Buffer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestStreamingNativeExecutionTask
{
    private static final URI BASE_URI = URI.create("http://localhost:8080");
    private static final JsonCodec<TaskInfo> TASK_INFO_JSON_CODEC = JsonCodec.jsonCodec(TaskInfo.class);
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<BatchTaskUpdateRequest> TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(BatchTaskUpdateRequest.class);
    private static final JsonCodec<StreamingBatchTaskUpdateRequest> STREAMING_TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(StreamingBatchTaskUpdateRequest.class);
    // The task update request may read Presto-on-Spark session properties, which the default testing session does not register
    private static final SessionPropertyManager SESSION_PROPERTY_MANAGER = createTestingSessionPropertyManager(
            ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new SystemSessionProperties().getSessionProperties())
                    .addAll(new PrestoSparkSessionProperties().getSessionProperties())
                    .build());

    private ScheduledExecutorService scheduledExecutorService;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutorService = newScheduledThreadPool(4);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService = null;
    }

    @Test
    public void testSendsSameRequestAsNonStreamingTask()
    {
        Session session = testSessionBuilder(SESSION_PROPERTY_MANAGER).build();
        PlanFragment fragment = createPlanFragment();
        List<TaskSource> sources = sources();

        RecordingTaskInfoResponseManager nonStreaming = new RecordingTaskInfoResponseManager();
        NativeExecutionTask task = taskFactory(nonStreaming).createNativeExecutionTask(
                session, BASE_URI, taskId(), fragment, sources, new TableWriteInfo(Optional.empty(), Optional.empty()),
                Optional.empty(), Optional.empty(), false);
        startAndStop(task);

        RecordingTaskInfoResponseManager streaming = new RecordingTaskInfoResponseManager();
        NativeExecutionTask streamingTask = taskFactory(streaming).createStreamingNativeExecutionTask(
                session, BASE_URI, taskId(), fragment, sources.iterator(), new TableWriteInfo(Optional.empty(), Optional.empty()),
                Optional.empty(), Optional.empty(), false, STREAMING_TASK_UPDATE_REQUEST_JSON_CODEC);
        startAndStop(streamingTask);

        assertEquals(readBody(streaming.getRequest()), readBody(nonStreaming.getRequest()));
    }

    @Test
    public void testUpdateRequestIsSentOnce()
    {
        NativeExecutionTask streamingTask = taskFactory(new RecordingTaskInfoResponseManager()).createStreamingNativeExecutionTask(
                testSessionBuilder(SESSION_PROPERTY_MANAGER).build(), BASE_URI, taskId(), createPlanFragment(), sources().iterator(),
                new TableWriteInfo(Optional.empty(), Optional.empty()), Optional.empty(), Optional.empty(), false,
                STREAMING_TASK_UPDATE_REQUEST_JSON_CODEC);
        try {
            streamingTask.start();
            // The source stream is single-pass and has been dropped; a second send must fail rather
            // than post a request without splits.
            assertThatThrownBy(streamingTask::sendUpdateRequest)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already sent its update request");
        }
        finally {
            streamingTask.stop(false);
        }
    }

    @Test
    public void testSourceFailureIsNotWrapped()
    {
        PrestoException failure = new PrestoException(GENERIC_INTERNAL_ERROR, "cannot build task source");
        Iterator<TaskSource> failingSources = new AbstractIterator<TaskSource>()
        {
            @Override
            protected TaskSource computeNext()
            {
                throw failure;
            }
        };
        NativeExecutionTask streamingTask = taskFactory(new RecordingTaskInfoResponseManager()).createStreamingNativeExecutionTask(
                testSessionBuilder(SESSION_PROPERTY_MANAGER).build(), BASE_URI, taskId(), createPlanFragment(), failingSources,
                new TableWriteInfo(Optional.empty(), Optional.empty()), Optional.empty(), Optional.empty(), false,
                STREAMING_TASK_UPDATE_REQUEST_JSON_CODEC);
        try {
            // Thrown as is, as the non-streaming path would throw it while building the sources.
            assertThatThrownBy(streamingTask::start).isSameAs(failure);
        }
        finally {
            streamingTask.stop(false);
        }
    }

    private NativeExecutionTaskFactory taskFactory(RecordingTaskInfoResponseManager responseManager)
    {
        return new NativeExecutionTaskFactory(
                new TestingOkHttpClient(scheduledExecutorService, new TestingResponseManager(taskId().toString(), responseManager)),
                scheduledExecutorService,
                scheduledExecutorService,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new TaskManagerConfig(),
                new QueryManagerConfig());
    }

    private static void startAndStop(NativeExecutionTask task)
    {
        try {
            task.start();
        }
        finally {
            task.stop(false);
        }
    }

    private static TaskId taskId()
    {
        return new TaskId("testid", 0, 0, 0, 0);
    }

    private static List<TaskSource> sources()
    {
        return ImmutableList.of(
                new TaskSource(new PlanNodeId("tableScan"), ImmutableSet.of(), true),
                new TaskSource(new PlanNodeId("remoteSource"), ImmutableSet.of(), true));
    }

    private static byte[] readBody(Request request)
    {
        assertNotNull(request);
        Buffer buffer = new Buffer();
        try {
            request.body().writeTo(buffer);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return buffer.readByteArray();
    }

    private static class RecordingTaskInfoResponseManager
            extends TestingResponseManager.TestingTaskInfoResponseManager
    {
        private final AtomicReference<Request> request = new AtomicReference<>();

        @Override
        public Response createTaskInfoResponse(int httpStatus, String taskId, Request request)
                throws PrestoException
        {
            // Only the task update is a POST; keep the first one.
            if (request.method().equalsIgnoreCase("POST")) {
                this.request.compareAndSet(null, request);
            }
            return super.createTaskInfoResponse(httpStatus, taskId, request);
        }

        Request getRequest()
        {
            return request.get();
        }
    }
}
