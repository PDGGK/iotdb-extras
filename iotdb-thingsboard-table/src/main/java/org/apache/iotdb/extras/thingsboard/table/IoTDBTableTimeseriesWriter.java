/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.extras.thingsboard.table;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.springframework.beans.factory.DisposableBean;
import org.thingsboard.server.common.data.kv.DataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class IoTDBTableTimeseriesWriter implements DisposableBean {
  static final String TABLE_NAME = "telemetry";
  static final List<String> COLUMN_NAMES =
      List.of(
          "tenant_id",
          "entity_type",
          "entity_id",
          "key",
          "bool_v",
          "long_v",
          "double_v",
          "str_v",
          "json_v");
  static final List<TSDataType> DATA_TYPES =
      List.of(
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.BOOLEAN,
          TSDataType.INT64,
          TSDataType.DOUBLE,
          TSDataType.STRING,
          TSDataType.TEXT);
  static final List<ColumnCategory> COLUMN_CATEGORIES =
      List.of(
          ColumnCategory.TAG,
          ColumnCategory.TAG,
          ColumnCategory.TAG,
          ColumnCategory.TAG,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD);

  private final ITableSessionPool tableSessionPool;
  private final BlockingQueue<IoTDBTablePendingSave> queue;
  private final int batchSize;
  private final long maxLingerNanos;
  private final long shutdownDrainTimeoutMs;
  private final int retryMaxAttempts;
  private final long retryInitialBackoffMs;
  private final long retryMaxBackoffMs;
  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicBoolean destroyed = new AtomicBoolean(false);
  private final Object activeBatchLock = new Object();
  private final AtomicLong enqueued = new AtomicLong();
  private final AtomicLong flushed = new AtomicLong();
  private final AtomicLong flushFailures = new AtomicLong();
  private final AtomicLong retries = new AtomicLong();
  private final AtomicLong rejectsFull = new AtomicLong();
  private final AtomicLong rejectsShutdown = new AtomicLong();
  private final AtomicLong shutdownFailedPending = new AtomicLong();
  private final AtomicLong queueDepth = new AtomicLong();
  private volatile List<IoTDBTablePendingSave> activeBatch = List.of();
  private final Thread worker;

  public IoTDBTableTimeseriesWriter(ITableSessionPool tableSessionPool, IoTDBTableConfig config) {
    this(tableSessionPool, config, true);
  }

  IoTDBTableTimeseriesWriter(
      ITableSessionPool tableSessionPool, IoTDBTableConfig config, boolean startWorker) {
    this(
        tableSessionPool,
        config,
        startWorker,
        new ArrayBlockingQueue<>(config.getTs().getSave().getQueueCapacity()));
  }

  IoTDBTableTimeseriesWriter(
      ITableSessionPool tableSessionPool,
      IoTDBTableConfig config,
      boolean startWorker,
      BlockingQueue<IoTDBTablePendingSave> queue) {
    this.tableSessionPool = Objects.requireNonNull(tableSessionPool, "tableSessionPool");
    Objects.requireNonNull(config, "config");
    IoTDBTableConfig.Save saveConfig = config.getTs().getSave();
    this.batchSize = saveConfig.getBatchSize();
    this.maxLingerNanos = TimeUnit.MILLISECONDS.toNanos(saveConfig.getMaxLingerMs());
    this.shutdownDrainTimeoutMs = saveConfig.getShutdownDrainTimeoutMs();
    this.retryMaxAttempts = saveConfig.getRetryMaxAttempts();
    this.retryInitialBackoffMs = saveConfig.getRetryInitialBackoffMs();
    this.retryMaxBackoffMs = saveConfig.getRetryMaxBackoffMs();
    this.queue = Objects.requireNonNull(queue, "queue");
    this.worker = new Thread(this::runFlushLoop, "iotdb-table-timeseries-flush-worker");
    this.worker.setDaemon(true);
    if (startWorker) {
      this.worker.start();
    }
  }

  public ListenableFuture<Integer> enqueue(IoTDBTablePendingSave pending) {
    Objects.requireNonNull(pending, "pending");
    if (!accepting.get()) {
      rejectsShutdown.incrementAndGet();
      pending.future().setException(shuttingDownException());
      return pending.future();
    }
    if (!queue.offer(pending)) {
      rejectsFull.incrementAndGet();
      pending
          .future()
          .setException(
              new IoTDBTableSaveQueueFullException(
                  "IoTDB Table Mode timeseries save queue is full"));
      return pending.future();
    }
    enqueued.incrementAndGet();
    queueDepth.incrementAndGet();
    if (!accepting.get() && queue.remove(pending)) {
      queueDepth.decrementAndGet();
      rejectsShutdown.incrementAndGet();
      pending.future().setException(shuttingDownException());
    }
    return pending.future();
  }

  public IoTDBTableTimeseriesWriterStats stats() {
    return new IoTDBTableTimeseriesWriterStats(
        enqueued.get(),
        flushed.get(),
        flushFailures.get(),
        retries.get(),
        rejectsFull.get(),
        rejectsShutdown.get(),
        shutdownFailedPending.get(),
        queueDepth.get());
  }

  /**
   * Stops accepting saves and gives the flush worker a bounded drain window.
   *
   * <p>If that window expires while an insert is in flight, pending futures are failed fast even
   * though IoTDB may still commit the final batch. Shutdown therefore remains at least once:
   * callers must tolerate duplicate or uncertain final-batch writes.
   */
  @Override
  public void destroy() {
    if (!destroyed.compareAndSet(false, true)) {
      return;
    }
    accepting.set(false);
    // Do NOT interrupt the worker up front. An in-flight retry backoff (Thread.sleep in
    // sleepBeforeRetry) would throw InterruptedException, flushBatch's catch would fail the
    // already-accepted batch, and the drain window would be wasted on exactly the
    // transient-error-during-shutdown path it exists to protect. With accepting=false the worker
    // observes shutdown within one poll cycle (<=100ms), lets any in-flight retry finish, and
    // drains
    // the queue + current batch on its own. Only if it is STILL alive after the drain timeout
    // (below)
    // do we interrupt and fail whatever remains.
    try {
      worker.join(shutdownDrainTimeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (worker.isAlive()) {
      failUnfinishedPending(
          new IoTDBTableDaoShuttingDownException(
              "IoTDB Table Mode timeseries DAO shutdown drain timed out"));
      worker.interrupt();
    } else if (!queue.isEmpty()) {
      failUnfinishedPending(
          new IoTDBTableDaoShuttingDownException(
              "IoTDB Table Mode timeseries DAO stopped before draining pending writes"));
    }
  }

  private void runFlushLoop() {
    List<IoTDBTablePendingSave> batch = new ArrayList<>(batchSize);
    while (accepting.get() || !queue.isEmpty() || !batch.isEmpty()) {
      try {
        if (batch.isEmpty()) {
          IoTDBTablePendingSave first = queue.poll(100L, TimeUnit.MILLISECONDS);
          if (first == null) {
            continue;
          }
          addToBatch(batch, first);
        }
        fillBatchUntilReady(batch);
        if (!batch.isEmpty()) {
          flushBatch(batch);
          batch.clear();
        }
      } catch (InterruptedException e) {
        if (accepting.get()) {
          Thread.currentThread().interrupt();
          failBatch(
              batch,
              new IoTDBTableDaoShuttingDownException(
                  "IoTDB Table Mode timeseries flush worker was interrupted"));
          batch.clear();
          break;
        }
      } catch (RuntimeException e) {
        failBatch(batch, e);
        batch.clear();
      } catch (Throwable t) {
        failBatch(batch, t);
        batch.clear();
      }
    }
  }

  // Upper bound on each linger-poll slice, so an in-flight linger wait observes a concurrent
  // shutdown
  // (accepting=false) within one slice instead of waiting out a large maxLingerMs -- WITHOUT
  // interrupting the worker (an interrupt would abort an in-flight retry backoff; see destroy()).
  private static final long SHUTDOWN_OBSERVE_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

  private void fillBatchUntilReady(List<IoTDBTablePendingSave> batch) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + maxLingerNanos;
    while (batch.size() < batchSize) {
      // Once shutdown has begun, stop lingering and flush the partial batch immediately: any items
      // still queued are drained by the outer runFlushLoop (its loop condition includes
      // !queue.isEmpty()), so no accepted write is lost and the drain window is not spent waiting
      // out
      // maxLingerMs. (Returning here also keeps the pre-flush batch invisible to
      // failUnfinishedPending
      // for only the few instructions until flushBatch publishes activeBatch.)
      if (!accepting.get()) {
        return;
      }
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0L) {
        return;
      }
      // Poll in bounded slices so a shutdown that races an in-flight linger wait is observed within
      // one slice. With the default small maxLingerMs the slice covers the whole wait (single poll,
      // unchanged behaviour); only a large maxLingerMs is sliced.
      long sliceNanos = Math.min(remainingNanos, SHUTDOWN_OBSERVE_SLICE_NANOS);
      IoTDBTablePendingSave next = queue.poll(sliceNanos, TimeUnit.NANOSECONDS);
      if (next == null) {
        // Slice elapsed with no item: re-check accepting + the maxLinger deadline at the loop top.
        continue;
      }
      addToBatch(batch, next);
      drainAvailable(batch);
    }
  }

  private void drainAvailable(List<IoTDBTablePendingSave> batch) {
    int limit = batchSize - batch.size();
    if (limit <= 0) {
      return;
    }
    List<IoTDBTablePendingSave> drained = new ArrayList<>(limit);
    int drainedCount = queue.drainTo(drained, limit);
    if (drainedCount == 0) {
      return;
    }
    queueDepth.addAndGet(-drainedCount);
    batch.addAll(drained);
  }

  private void addToBatch(List<IoTDBTablePendingSave> batch, IoTDBTablePendingSave pending) {
    queueDepth.decrementAndGet();
    batch.add(pending);
  }

  private void flushBatch(List<IoTDBTablePendingSave> rawBatch) {
    List<IoTDBTablePendingSave> insertBatch = deduplicateForInsert(rawBatch);
    if (insertBatch.isEmpty()) {
      return;
    }
    synchronized (activeBatchLock) {
      activeBatch = List.copyOf(rawBatch);
    }
    try {
      Tablet tablet = buildTablet(insertBatch);
      insertWithRetry(tablet);
      flushed.addAndGet(insertBatch.size());
      completeBatch(rawBatch);
    } catch (Throwable t) {
      flushFailures.incrementAndGet();
      failBatch(rawBatch, t);
    } finally {
      synchronized (activeBatchLock) {
        activeBatch = List.of();
      }
    }
  }

  private List<IoTDBTablePendingSave> deduplicateForInsert(List<IoTDBTablePendingSave> rawBatch) {
    // TODO(GSOC-304 §3.4): Phase-1 relaxation mentor-approved (2026-06-01);
    // defer cross-batch same-ts type-change delete-then-insert defense.
    Map<IoTDBTableSaveIdentity, IoTDBTablePendingSave> lastByIdentity =
        new LinkedHashMap<>(rawBatch.size());
    for (IoTDBTablePendingSave pending : rawBatch) {
      IoTDBTableSaveIdentity identity = pending.identity();
      lastByIdentity.remove(identity);
      lastByIdentity.put(identity, pending);
    }
    return new ArrayList<>(lastByIdentity.values());
  }

  Tablet buildTablet(List<IoTDBTablePendingSave> batch) {
    Tablet tablet =
        new Tablet(TABLE_NAME, COLUMN_NAMES, DATA_TYPES, COLUMN_CATEGORIES, batch.size());
    for (int row = 0; row < batch.size(); row++) {
      IoTDBTablePendingSave pending = batch.get(row);
      tablet.addTimestamp(row, pending.ts());
      tablet.addValue("tenant_id", row, pending.tenantId());
      tablet.addValue("entity_type", row, pending.entityType());
      tablet.addValue("entity_id", row, pending.entityId());
      tablet.addValue("key", row, pending.key());
      tablet.addValue(
          "bool_v", row, pending.dataType() == DataType.BOOLEAN ? pending.value() : null);
      tablet.addValue("long_v", row, pending.dataType() == DataType.LONG ? pending.value() : null);
      tablet.addValue(
          "double_v", row, pending.dataType() == DataType.DOUBLE ? pending.value() : null);
      tablet.addValue("str_v", row, pending.dataType() == DataType.STRING ? pending.value() : null);
      tablet.addValue("json_v", row, pending.dataType() == DataType.JSON ? pending.value() : null);
    }
    tablet.setRowSize(batch.size());
    return tablet;
  }

  private void insertWithRetry(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    long backoffMs = retryInitialBackoffMs;
    for (int attempt = 1; attempt <= retryMaxAttempts; attempt++) {
      boolean inserted = false;
      try (ITableSession session = tableSessionPool.getSession()) {
        session.insert(tablet);
        inserted = true;
        return;
      } catch (IoTDBConnectionException e) {
        if (inserted) {
          // close() failed after insert returned; do not replay a tablet that may be persisted.
          return;
        }
        if (attempt >= retryMaxAttempts) {
          throw e;
        }
        retries.incrementAndGet();
        sleepBeforeRetry(backoffMs);
        backoffMs = nextBackoffMs(backoffMs);
      }
    }
    throw new IllegalStateException("IoTDB insert retry loop exited without success or failure");
  }

  private IoTDBTableDaoShuttingDownException shuttingDownException() {
    return new IoTDBTableDaoShuttingDownException(
        "IoTDB Table Mode timeseries DAO is shutting down");
  }

  private void sleepBeforeRetry(long backoffMs) throws InterruptedException {
    if (backoffMs > 0L) {
      Thread.sleep(backoffMs);
    }
  }

  private long nextBackoffMs(long currentBackoffMs) {
    if (retryMaxBackoffMs <= 0L) {
      return 0L;
    }
    if (currentBackoffMs <= 0L) {
      return Math.min(1L, retryMaxBackoffMs);
    }
    long doubled = currentBackoffMs > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : currentBackoffMs * 2L;
    return Math.min(retryMaxBackoffMs, doubled);
  }

  private void completeBatch(List<IoTDBTablePendingSave> batch) {
    for (IoTDBTablePendingSave pending : batch) {
      pending.future().set(pending.dataPointDays());
    }
  }

  private void failBatch(List<IoTDBTablePendingSave> batch, Throwable t) {
    for (IoTDBTablePendingSave pending : batch) {
      pending.future().setException(t);
    }
  }

  private void failUnfinishedPending(RuntimeException failure) {
    List<IoTDBTablePendingSave> pending = new ArrayList<>();
    queue.drainTo(pending);
    queueDepth.addAndGet(-pending.size());
    synchronized (activeBatchLock) {
      pending.addAll(activeBatch);
    }
    long failed = 0L;
    for (IoTDBTablePendingSave save : pending) {
      if (save.future().setException(failure)) {
        failed++;
      }
    }
    shutdownFailedPending.addAndGet(failed);
  }
}
