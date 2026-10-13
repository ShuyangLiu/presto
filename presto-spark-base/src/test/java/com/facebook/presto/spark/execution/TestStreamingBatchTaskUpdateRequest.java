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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.StreamingBatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.StreamingBatchTaskUpdateRequest.StreamingTaskUpdateRequest;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * The streaming request exists only to produce the same bytes as {@link BatchTaskUpdateRequest}
 * while holding fewer splits alive. These tests pin both halves of that: the output is identical,
 * and the sources really are pulled during serialization rather than before it.
 *
 * <p>The equality test is also the guard against field drift -- a property added to
 * {@link TaskUpdateRequest} or {@link BatchTaskUpdateRequest} and not mirrored on the streaming
 * types shows up here as a diff, not as a native worker rejecting the request in production.
 */
public class TestStreamingBatchTaskUpdateRequest
{
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final String SHUFFLE_WRITE_INFO = "dummy-shuffle-write-info";

    @Test
    public void testMatchesNonStreamingSerialization()
            throws Exception
    {
        Injector injector = createInjector();
        JsonCodec<BatchTaskUpdateRequest> batchCodec = injector.getInstance(new Key<JsonCodec<BatchTaskUpdateRequest>>() {});
        JsonCodec<StreamingBatchTaskUpdateRequest> streamingCodec = injector.getInstance(new Key<JsonCodec<StreamingBatchTaskUpdateRequest>>() {});

        Session session = TestingSession.testSessionBuilder().build();
        List<TaskSource> sources = createSources(3);

        byte[] expected = batchCodec.toBytes(createBatchRequest(session, sources));
        byte[] actual = streamingCodec.toBytes(createStreamingRequest(session, sources.iterator()));

        assertEquals(new String(actual, UTF_8), new String(expected, UTF_8));
    }

    @Test
    public void testMatchesNonStreamingSerializationWithNoSources()
            throws Exception
    {
        Injector injector = createInjector();
        JsonCodec<BatchTaskUpdateRequest> batchCodec = injector.getInstance(new Key<JsonCodec<BatchTaskUpdateRequest>>() {});
        JsonCodec<StreamingBatchTaskUpdateRequest> streamingCodec = injector.getInstance(new Key<JsonCodec<StreamingBatchTaskUpdateRequest>>() {});

        Session session = TestingSession.testSessionBuilder().build();
        List<TaskSource> sources = ImmutableList.of();

        byte[] expected = batchCodec.toBytes(createBatchRequest(session, sources));
        byte[] actual = streamingCodec.toBytes(createStreamingRequest(session, sources.iterator()));

        assertEquals(new String(actual, UTF_8), new String(expected, UTF_8));
    }

    /**
     * The whole point: nothing pulls the sources until the bytes are being written. If something
     * upstream drained the iterator first, the peak this class exists to remove would be back.
     */
    @Test
    public void testSourcesArePulledDuringSerialization()
            throws Exception
    {
        Injector injector = createInjector();
        JsonCodec<StreamingBatchTaskUpdateRequest> streamingCodec = injector.getInstance(new Key<JsonCodec<StreamingBatchTaskUpdateRequest>>() {});

        Session session = TestingSession.testSessionBuilder().build();
        AtomicInteger pulled = new AtomicInteger();
        Iterator<TaskSource> counting = countingIterator(createSources(4).iterator(), pulled);

        StreamingBatchTaskUpdateRequest request = createStreamingRequest(session, counting);
        assertEquals(pulled.get(), 0, "sources were consumed before serialization");

        streamingCodec.toBytes(request);
        assertEquals(pulled.get(), 4);
    }

    @Test
    public void testStreamedBytesDeserializeAsBatchRequest()
            throws Exception
    {
        Injector injector = createInjector();
        JsonCodec<BatchTaskUpdateRequest> batchCodec = injector.getInstance(new Key<JsonCodec<BatchTaskUpdateRequest>>() {});
        JsonCodec<StreamingBatchTaskUpdateRequest> streamingCodec = injector.getInstance(new Key<JsonCodec<StreamingBatchTaskUpdateRequest>>() {});

        Session session = TestingSession.testSessionBuilder().build();
        byte[] streamed = streamingCodec.toBytes(createStreamingRequest(session, createSources(2).iterator()));

        BatchTaskUpdateRequest recovered = batchCodec.fromBytes(streamed);
        List<TaskSource> recoveredSources = recovered.getTaskUpdateRequest().getSources();
        assertEquals(recoveredSources.size(), 2);
        assertEquals(recoveredSources.get(0).getPlanNodeId(), new PlanNodeId("planNodeId0"));
        assertEquals(recoveredSources.get(1).getPlanNodeId(), new PlanNodeId("planNodeId1"));
        assertTrue(((ScheduledSplit) recoveredSources.get(0).getSplits().toArray()[0]).getSplit().getConnectorSplit() instanceof RemoteSplit);
        assertEquals(recovered.getShuffleWriteInfo().get(), SHUFFLE_WRITE_INFO);
    }

    private static BatchTaskUpdateRequest createBatchRequest(Session session, List<TaskSource> sources)
    {
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                Optional.of(createPlanFragment().bytesForTaskSerialization(PLAN_FRAGMENT_JSON_CODEC)),
                sources,
                createInitialEmptyOutputBuffers(PARTITIONED),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty())));
        return new BatchTaskUpdateRequest(updateRequest, Optional.of(SHUFFLE_WRITE_INFO), Optional.empty());
    }

    private static StreamingBatchTaskUpdateRequest createStreamingRequest(Session session, Iterator<TaskSource> sources)
    {
        StreamingTaskUpdateRequest updateRequest = new StreamingTaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                Optional.of(createPlanFragment().bytesForTaskSerialization(PLAN_FRAGMENT_JSON_CODEC)),
                sources,
                createInitialEmptyOutputBuffers(PARTITIONED),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty())));
        return new StreamingBatchTaskUpdateRequest(updateRequest, Optional.of(SHUFFLE_WRITE_INFO), Optional.empty());
    }

    private static List<TaskSource> createSources(int count)
    {
        ImmutableList.Builder<TaskSource> sources = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            PlanNodeId planNodeId = new PlanNodeId("planNodeId" + i);
            sources.add(new TaskSource(
                    planNodeId,
                    ImmutableSet.of(new ScheduledSplit(
                            i,
                            planNodeId,
                            new Split(
                                    new ConnectorId("connector_id"),
                                    new RemoteTransactionHandle(),
                                    new RemoteSplit(new Location("batch://dummy?shuffleInfo=" + i), TaskId.valueOf("foo.0.0.0.0"))))),
                    true));
        }
        return sources.build();
    }

    private static Iterator<TaskSource> countingIterator(Iterator<TaskSource> delegate, AtomicInteger counter)
    {
        return new Iterator<TaskSource>()
        {
            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public TaskSource next()
            {
                counter.incrementAndGet();
                return delegate.next();
            }
        };
    }

    private static Injector createInjector()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
            jsonCodecBinder(binder).bindJsonCodec(BatchTaskUpdateRequest.class);
            jsonCodecBinder(binder).bindJsonCodec(StreamingBatchTaskUpdateRequest.class);
        };
        return new Bootstrap(ImmutableList.of(module))
                .doNotInitializeLogging()
                .quiet()
                .initialize();
    }
}
