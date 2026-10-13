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
package com.facebook.presto.spark.execution.http;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.server.TaskUpdateRequest;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Serialization-only mirror of {@link BatchTaskUpdateRequest} whose task sources are pulled from
 * an {@link Iterator} instead of a materialized {@link java.util.List}.
 *
 * <p>Jackson serializes an {@code Iterator}-typed property with {@code IteratorSerializer}, which
 * pulls one element, writes it, and drops the reference. Paired with an iterator that deserializes
 * each {@link TaskSource} on demand, the whole split set is never alive at once: peak heap for the
 * request becomes one TaskSource plus the output buffer, rather than every TaskSource plus the
 * output buffer.
 *
 * <p>This exists rather than a lazy {@code List} because {@link TaskUpdateRequest} defensively
 * copies its sources with {@code ImmutableList.copyOf}, which would drain any lazy list back into
 * a fully materialized one. It cannot be a subclass of {@code TaskUpdateRequest} either: the copy
 * happens in the super constructor, before an override could take effect.
 *
 * <p>The JSON shape is byte-for-byte the shape {@code BatchTaskUpdateRequest} produces -- the
 * native worker parses one type for both paths, and {@code TestStreamingBatchTaskUpdateRequest}
 * asserts the two serializations agree. Any field added to {@link TaskUpdateRequest} or
 * {@link BatchTaskUpdateRequest} must be mirrored here; that test is what catches the omission.
 *
 * <p>Single use. The sources iterator is consumed by the first serialization, so an instance must
 * not be serialized twice. Callers retry the resulting bytes, never this object.
 */
// Marked as final to prevent finalizer attack. See Guideline 7-3 in java secure coding guidelines
public final class StreamingBatchTaskUpdateRequest
{
    private final StreamingTaskUpdateRequest taskUpdateRequest;
    private final Optional<String> shuffleWriteInfo;
    private final Optional<String> broadcastBasePath;

    public StreamingBatchTaskUpdateRequest(
            StreamingTaskUpdateRequest taskUpdateRequest,
            Optional<String> shuffleWriteInfo,
            Optional<String> broadcastBasePath)
    {
        this.taskUpdateRequest = requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        this.shuffleWriteInfo = requireNonNull(shuffleWriteInfo, "shuffleWriteInfo is null");
        this.broadcastBasePath = requireNonNull(broadcastBasePath, "broadcastBasePath is null");

        // shuffleWriteInfo and broadcastBasePath, both can't have value at the same time.
        if (this.shuffleWriteInfo.isPresent() && this.broadcastBasePath.isPresent()) {
            throw new IllegalArgumentException("shuffleWriteInfo and broadcastBasePath can not be specified in same request");
        }
    }

    @JsonProperty
    public StreamingTaskUpdateRequest getTaskUpdateRequest()
    {
        return taskUpdateRequest;
    }

    @JsonProperty
    public Optional<String> getShuffleWriteInfo()
    {
        return shuffleWriteInfo;
    }

    @JsonProperty
    public Optional<String> getBroadcastBasePath()
    {
        return broadcastBasePath;
    }

    @Override
    public String toString()
    {
        ToStringHelper stringHelper = toStringHelper(this)
                .add("taskUpdateRequest", taskUpdateRequest);
        shuffleWriteInfo.ifPresent(shuffleInfo -> stringHelper.add("shuffleWriteInfo", shuffleInfo));
        broadcastBasePath.ifPresent(basePath -> stringHelper.add("broadcastBasePath", basePath));
        return stringHelper.toString();
    }

    /**
     * Mirror of {@link TaskUpdateRequest}, property for property and in the same order, with
     * {@code sources} streamed. See the outer class for why this is a separate type.
     */
    // Marked as final to prevent finalizer attack. See Guideline 7-3 in java secure coding guidelines
    public static final class StreamingTaskUpdateRequest
    {
        private final SessionRepresentation session;
        private final Map<String, String> extraCredentials;
        private final Optional<byte[]> fragment;
        private final Iterator<TaskSource> sources;
        private final OutputBuffers outputIds;
        private final Optional<TableWriteInfo> tableWriteInfo;

        public StreamingTaskUpdateRequest(
                SessionRepresentation session,
                Map<String, String> extraCredentials,
                Optional<byte[]> fragment,
                Iterator<TaskSource> sources,
                OutputBuffers outputIds,
                Optional<TableWriteInfo> tableWriteInfo)
        {
            this.session = requireNonNull(session, "session is null");
            this.extraCredentials = requireNonNull(extraCredentials, "extraCredentials is null");
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.sources = requireNonNull(sources, "sources is null");
            this.outputIds = requireNonNull(outputIds, "outputIds is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        }

        @JsonProperty
        public SessionRepresentation getSession()
        {
            return session;
        }

        @JsonProperty
        public Map<String, String> getExtraCredentials()
        {
            return extraCredentials;
        }

        @JsonInclude(NON_ABSENT)
        @JsonProperty
        public Optional<byte[]> getFragment()
        {
            return fragment;
        }

        /**
         * Consumed by serialization. Calling this twice yields an exhausted iterator and a request
         * with no splits, which the native worker rejects in checkSplitsForBatchTask.
         */
        @JsonProperty
        public Iterator<TaskSource> getSources()
        {
            return sources;
        }

        @JsonProperty
        public OutputBuffers getOutputIds()
        {
            return outputIds;
        }

        @JsonProperty
        public Optional<TableWriteInfo> getTableWriteInfo()
        {
            return tableWriteInfo;
        }

        @Override
        public String toString()
        {
            // Deliberately omits the sources: reading them here would consume the iterator.
            return toStringHelper(this)
                    .add("session", session)
                    .add("extraCredentials", extraCredentials.keySet())
                    .add("fragment", fragment)
                    .add("outputIds", outputIds)
                    .toString();
        }
    }
}
