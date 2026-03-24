/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * Stream operator that wraps a {@link DynamicWriter} directly, emitting {@link CommittableMessage}s
 * on checkpoint without requiring the Sink/SupportsCommitter interfaces.
 *
 * <p>Replaces the previous WriterSink + SinkWriterOperatorFactory approach for the forward write
 * path, keeping the committable emission protocol (CommittableSummary + CommittableWithLineage)
 * compatible with downstream operators like {@link DynamicWriteResultAggregator}.
 */
class DynamicWriterOperator
    extends AbstractStreamOperator<CommittableMessage<DynamicWriteResult>>
    implements OneInputStreamOperator<
            DynamicRecordInternal, CommittableMessage<DynamicWriteResult>>,
        BoundedOneInput {

  private final CatalogLoader catalogLoader;
  private final Map<String, String> writeProperties;
  private final Configuration flinkConfig;
  private final int cacheMaximumSize;

  private transient DynamicWriter writer;
  private long lastCheckpointId = CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1;

  DynamicWriterOperator(
      CatalogLoader catalogLoader,
      Map<String, String> writeProperties,
      Configuration flinkConfig,
      int cacheMaximumSize) {
    this.catalogLoader = catalogLoader;
    this.writeProperties = writeProperties;
    this.flinkConfig = flinkConfig;
    this.cacheMaximumSize = cacheMaximumSize;
  }

  @Override
  public void open() throws Exception {
    Catalog catalog = catalogLoader.loadCatalog();
    int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int attemptNumber = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.writer =
        new DynamicWriter(
            catalog,
            writeProperties,
            flinkConfig,
            cacheMaximumSize,
            new DynamicWriterMetrics(InternalSinkWriterMetricGroup.wrap(getMetricGroup())),
            subtaskIndex,
            attemptNumber);
  }

  @Override
  public void processElement(StreamRecord<DynamicRecordInternal> element) throws Exception {
    writer.write(element.getValue(), null);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    writer.flush(false);
    emitCommittables(checkpointId);
  }

  @Override
  public void endInput() throws IOException {
    writer.flush(true);
    emitCommittables(lastCheckpointId + 1);
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  private void emitCommittables(long checkpointId) throws IOException {
    lastCheckpointId = checkpointId;
    int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

    Collection<DynamicWriteResult> committables = writer.prepareCommit();

    output.collect(
        new StreamRecord<>(
            new CommittableSummary<>(
                subtaskId,
                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
                checkpointId,
                committables.size(),
                committables.size(),
                0)));

    for (DynamicWriteResult committable : committables) {
      output.collect(
          new StreamRecord<>(
              new CommittableWithLineage<>(committable, checkpointId, subtaskId)));
    }
  }

  @VisibleForTesting
  DynamicWriter getWriter() {
    return writer;
  }
}
