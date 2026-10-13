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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkConfig;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.execution.BroadcastFileInfo;
import com.facebook.presto.spark.execution.PrestoSparkBroadcastTableCacheManager;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.StreamingBatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingOkHttpClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingResponseManager;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionCatalogConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleInfoTranslator;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleReadInfo;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkLocalShuffleWriteInfo;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.spark.Partition;
import org.apache.spark.shuffle.ShuffleHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.collection.JavaConverters;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.facebook.presto.spi.plan.ExchangeEncoding.COLUMNAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;

/**
 * {@link PrestoSparkNativeTaskExecutorFactory#streamTaskSources} reads the table scan sources
 * separately from {@link PrestoSparkNativeTaskExecutorFactory#getTaskSources}. These tests pin the
 * two to the same output, including how the sources that follow are numbered.
 */
public class TestStreamTaskSources
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test");
    private static final VariableReferenceExpression VARIABLE = new VariableReferenceExpression(Optional.empty(), "column", BIGINT);
    private static final PlanNodeId SCAN_WITH_SPLITS = new PlanNodeId("scanWithSplits");
    private static final PlanNodeId SCAN_WITHOUT_SPLITS = new PlanNodeId("scanWithoutSplits");
    private static final PlanNodeId REMOTE_SOURCE = new PlanNodeId("remoteSource");
    private static final PlanFragmentId SOURCE_FRAGMENT = new PlanFragmentId(1);

    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService processExecutor;
    private JsonCodec<TaskSource> taskSourceCodec;
    private PrestoSparkNativeTaskExecutorFactory factory;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        scheduledExecutorService = newScheduledThreadPool(2);
        processExecutor = newSingleThreadExecutor();
        Injector injector = createInjector();
        taskSourceCodec = injector.getInstance(new Key<JsonCodec<TaskSource>>() {});
        TestingOkHttpClient httpClient = new TestingOkHttpClient(
                scheduledExecutorService,
                new TestingResponseManager("testid.0.0.0.0"));
        factory = new PrestoSparkNativeTaskExecutorFactory(
                createTestingSessionPropertyManager(),
                new BlockEncodingManager(),
                JsonCodec.jsonCodec(PrestoSparkTaskDescriptor.class),
                JsonCodec.jsonCodec(BroadcastFileInfo.class),
                taskSourceCodec,
                JsonCodec.jsonCodec(TaskInfo.class),
                new PrestoSparkExecutionExceptionFactory(JsonCodec.jsonCodec(ExecutionFailureInfo.class)),
                ImmutableSet.of(),
                new PrestoSparkBroadcastTableCacheManager(),
                new NativeExecutionProcessFactory(
                        httpClient,
                        processExecutor,
                        scheduledExecutorService,
                        JsonCodec.jsonCodec(ServerInfo.class),
                        new PrestoSparkWorkerProperty(
                                new NativeExecutionCatalogConfig(ImmutableMap.of()),
                                new NativeExecutionNodeConfig(),
                                new NativeExecutionSystemConfig(ImmutableMap.of())),
                        new FeaturesConfig()),
                new NativeExecutionTaskFactory(
                        httpClient,
                        scheduledExecutorService,
                        scheduledExecutorService,
                        JsonCodec.jsonCodec(TaskInfo.class),
                        JsonCodec.jsonCodec(PlanFragment.class),
                        JsonCodec.jsonCodec(BatchTaskUpdateRequest.class),
                        new TaskManagerConfig(),
                        new QueryManagerConfig()),
                new PrestoSparkLocalShuffleInfoTranslator(
                        JsonCodec.jsonCodec(PrestoSparkLocalShuffleReadInfo.class),
                        JsonCodec.jsonCodec(PrestoSparkLocalShuffleWriteInfo.class)),
                new TempStorageManager(new TestingNodeManager()),
                new PrestoSparkConfig(),
                new FeaturesConfig(),
                injector.getInstance(new Key<JsonCodec<StreamingBatchTaskUpdateRequest>>() {}));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService = null;
        processExecutor.shutdownNow();
        processExecutor = null;
        factory = null;
    }

    /**
     * Table scan sources with splits, a table scan that got none (so a no-more-splits source is
     * added), and a shuffle read, whose split is numbered after the highest table scan split.
     */
    @Test
    public void testSameSourcesAsGetTaskSources()
    {
        List<TaskSource> scanSources = ImmutableList.of(
                scanSource(SCAN_WITH_SPLITS, 7, 3),
                scanSource(SCAN_WITH_SPLITS, 5));
        PrestoSparkNativeTaskInputs inputs = new PrestoSparkNativeTaskInputs(
                ImmutableMap.of(SOURCE_FRAGMENT.toString(), shuffleReadDescriptor()),
                Optional.empty(),
                ImmutableMap.of());

        assertSameSources(scanSources, inputs);
    }

    @Test
    public void testSameSourcesWithoutTableScanSplits()
    {
        PrestoSparkNativeTaskInputs inputs = new PrestoSparkNativeTaskInputs(
                ImmutableMap.of(SOURCE_FRAGMENT.toString(), shuffleReadDescriptor()),
                Optional.empty(),
                ImmutableMap.of());

        assertSameSources(ImmutableList.of(), inputs);
    }

    @Test
    public void testSameSourcesWithoutRemoteSources()
    {
        PrestoSparkNativeTaskInputs inputs = new PrestoSparkNativeTaskInputs(ImmutableMap.of(), Optional.empty(), ImmutableMap.of());

        assertSameSources(ImmutableList.of(scanSource(SCAN_WITH_SPLITS, 2)), inputs);
    }

    private void assertSameSources(List<TaskSource> scanSources, PrestoSparkNativeTaskInputs inputs)
    {
        PlanFragment fragment = createPlanFragment();
        Session session = testSessionBuilder().build();

        List<TaskSource> expected = factory.getTaskSources(serialized(scanSources), fragment, session, inputs);
        Iterator<TaskSource> streamed = factory.streamTaskSources(serialized(scanSources), fragment, session, inputs);

        assertEquals(toJson(ImmutableList.copyOf(streamed)), toJson(expected));
    }

    private scala.collection.Iterator<SerializedPrestoSparkTaskSource> serialized(List<TaskSource> sources)
    {
        List<SerializedPrestoSparkTaskSource> serialized = sources.stream()
                .map(source -> new SerializedPrestoSparkTaskSource(serializeZstdCompressed(taskSourceCodec, source)))
                .collect(toImmutableList());
        return JavaConverters.asScalaIteratorConverter(serialized.iterator()).asScala();
    }

    private List<String> toJson(List<TaskSource> sources)
    {
        return sources.stream().map(taskSourceCodec::toJson).collect(toImmutableList());
    }

    private static TaskSource scanSource(PlanNodeId planNodeId, long... sequenceIds)
    {
        ImmutableSet.Builder<ScheduledSplit> splits = ImmutableSet.builder();
        for (long sequenceId : sequenceIds) {
            splits.add(new ScheduledSplit(
                    sequenceId,
                    planNodeId,
                    new Split(
                            CONNECTOR_ID,
                            new RemoteTransactionHandle(),
                            new RemoteSplit(new Location("batch://dummy?split=" + sequenceId), TaskId.valueOf("foo.0.0.0.0")))));
        }
        return new TaskSource(planNodeId, splits.build(), true);
    }

    private static PrestoSparkShuffleReadDescriptor shuffleReadDescriptor()
    {
        Partition partition = new Partition()
        {
            @Override
            public int index()
            {
                return 0;
            }
        };
        return new PrestoSparkShuffleReadDescriptor(
                partition,
                new ShuffleHandle(0) {},
                1,
                ImmutableList.of("block"),
                ImmutableList.of("partition"),
                ImmutableList.of(1L));
    }

    private static PlanFragment createPlanFragment()
    {
        PlanNode root = new UnionNode(
                Optional.empty(),
                new PlanNodeId("union"),
                ImmutableList.of(tableScan(SCAN_WITH_SPLITS), tableScan(SCAN_WITHOUT_SPLITS), new RemoteSourceNode(
                        Optional.empty(),
                        REMOTE_SOURCE,
                        ImmutableList.of(SOURCE_FRAGMENT),
                        ImmutableList.of(VARIABLE),
                        false,
                        Optional.empty(),
                        ExchangeNode.Type.REPARTITION,
                        COLUMNAR,
                        null)),
                ImmutableList.of(VARIABLE),
                ImmutableMap.of(VARIABLE, ImmutableList.of(VARIABLE, VARIABLE, VARIABLE)));
        return new PlanFragment(
                new PlanFragmentId(0),
                root,
                ImmutableSet.of(VARIABLE),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(SCAN_WITH_SPLITS, SCAN_WITHOUT_SPLITS),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(VARIABLE))
                        .withBucketToPartition(Optional.of(new int[1])),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }

    private static TableScanNode tableScan(PlanNodeId id)
    {
        return new TableScanNode(
                Optional.empty(),
                id,
                new TableHandle(CONNECTOR_ID, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(VARIABLE),
                ImmutableMap.of(VARIABLE, new TestingColumnHandle("column", 0, BIGINT)),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
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
            binder.bind(TypeManager.class).toInstance(createTestFunctionAndTypeManager());
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            jsonCodecBinder(binder).bindJsonCodec(TaskSource.class);
            jsonCodecBinder(binder).bindJsonCodec(StreamingBatchTaskUpdateRequest.class);
        };
        return new Bootstrap(ImmutableList.of(module))
                .doNotInitializeLogging()
                .quiet()
                .initialize();
    }
}
