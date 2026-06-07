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
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.BooleanDataEntry;
import org.thingsboard.server.common.data.kv.DeleteTsKvQuery;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.kv.TsKvLatestRemovingResult;
import org.thingsboard.server.dao.timeseries.TimeseriesLatestDao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Latest telemetry DAO for the IoTDB Table Mode backend.
 *
 * <p>Spring activation: {@code database.ts_latest.type=iotdb-table}.
 *
 * <p>Mentor-decided design (single-table derived latest, no shadow table): the latest value for an
 * entity/key is derived directly from the historical {@code telemetry} table. IoTDB Table Mode has
 * a native engine-level last cache (TableDeviceLastCache / LastQueryAggTableScanOperator), so the
 * {@code ORDER BY time DESC LIMIT 1} and {@code LAST_BY(col, time)} queries used here are
 * engine-accelerated. No separate {@code latest_telemetry} store is created or written.
 *
 * <p>Consequences of the no-shadow-table contract:
 *
 * <ul>
 *   <li>{@link #saveLatest} is a no-op: the paired aligned {@code save()} already wrote the row
 *       that the derived query naturally picks up. Same-timestamp overwrites are handled by IoTDB
 *       (writing the same {@code (tags, time)} overwrites the typed column).
 *   <li>{@link #removeLatest} performs no independent storage mutation: the historical {@code
 *       remove} path already deleted the underlying telemetry, and the derived latest follows.
 * </ul>
 *
 * <p>The key-discovery SPI methods ({@link #findAllKeysByDeviceProfileId}, {@link
 * #findAllKeysByEntityIds}, {@link #findAllKeysByEntityIdsAsync}) derive the distinct telemetry
 * keys from the telemetry table. A device-profile lookup with a {@code null} profile returns the
 * tenant-wide distinct keys; a specific profile returns no keys, because the telemetry table
 * carries no device-profile membership (that lives in ThingsBoard's relational entity database).
 *
 * @see "GSOC-304 design doc section 6.0"
 * @since GSOC-304 Wk 4 latest DAO
 */
@Slf4j
@Repository
@ConditionalOnBean(ITableSessionPool.class)
@ConditionalOnProperty(name = "database.ts_latest.type", havingValue = "iotdb-table")
public class IoTDBTableLatestDao extends IoTDBTableBaseDao
    implements TimeseriesLatestDao, DisposableBean {
  private static final String TABLE_NAME = IoTDBTableTimeseriesWriter.TABLE_NAME;
  private static final String SELECT_TYPED_COLUMNS =
      "time, bool_v, long_v, double_v, str_v, json_v";

  private final ThreadPoolExecutor readExecutor;
  private final java.util.Set<ReadTask<?>> readTasks = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicBoolean destroyed = new AtomicBoolean(false);
  private final long shutdownDrainTimeoutMs;

  public IoTDBTableLatestDao(ITableSessionPool tableSessionPool, IoTDBTableConfig config) {
    super(tableSessionPool);
    this.shutdownDrainTimeoutMs = config.getTs().getSave().getShutdownDrainTimeoutMs();
    int readThreads = config.getTs().getRead().getThreads();
    int readQueueCapacity = config.getTs().getRead().getQueueCapacity();
    this.readExecutor =
        new ThreadPoolExecutor(
            readThreads,
            readThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(readQueueCapacity),
            readThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy());
  }

  @Override
  public ListenableFuture<Optional<TsKvEntry>> findLatestOpt(
      TenantId tenantId, EntityId entityId, String key) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    String telemetryKey = requireTelemetryKey(key);
    return submitReadTask(() -> doFindLatest(tenantId, entityId, telemetryKey));
  }

  @Override
  public ListenableFuture<TsKvEntry> findLatest(TenantId tenantId, EntityId entityId, String key) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    String telemetryKey = requireTelemetryKey(key);
    return submitReadTask(
        () ->
            doFindLatest(tenantId, entityId, telemetryKey)
                .orElseGet(() -> nullEntry(telemetryKey)));
  }

  @Override
  public ListenableFuture<List<TsKvEntry>> findAllLatest(TenantId tenantId, EntityId entityId) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    return submitReadTask(() -> doFindAllLatest(tenantId, entityId));
  }

  @Override
  public ListenableFuture<Long> saveLatest(
      TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    Objects.requireNonNull(tsKvEntry, "tsKvEntry");
    // Single-table derived latest (no shadow table): the latest value is read straight from the
    // telemetry table that the paired aligned save() already populated, so there is no separate
    // latest store to update here. IoTDB resolves the mentor-approved "same-timestamp overwrite"
    // natively because writing the same (tags, time) overwrites the typed column in place. A null
    // Long version is returned (type-correct, matching the Cassandra backend's nullable version).
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<TsKvLatestRemovingResult> removeLatest(
      TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    Objects.requireNonNull(query, "query");
    String telemetryKey = requireTelemetryKey(query.getKey());
    // No independent storage mutation: the historical remove path already deleted the underlying
    // telemetry rows and the derived latest follows automatically. The result must still be HONEST
    // about whether a latest value was actually affected, because TB consumes isRemoved() as a real
    // delete signal (DefaultTelemetrySubscriptionService). Mirroring the reference
    // SqlTimeseriesLatestDao:
    // removed=true only when the current latest exists AND its timestamp falls inside the half-open
    // [startTs, endTs) delete window; otherwise removed=false.
    //
    // KNOWN Phase-1 limitation (no shadow latest table): TB's BaseTimeseriesService submits the
    // historical remove and this removeLatest as SEPARATE futures. If the historical delete commits
    // BEFORE this derived-latest read runs, the read sees an empty or older latest and reports
    // removed=FALSE even though the pre-delete latest WAS inside the delete window -- a false
    // negative
    // that suppresses the latest-delete notification. A robust fix needs a real latest shadow/state
    // to
    // capture a stable pre-delete snapshot (or a service-integration change to read the pre-delete
    // latest first); both are deferred Phase-1 and flagged for the mentor, since the single-table
    // derived-latest design (no shadow table) is the agreed Phase-1 approach.
    return submitReadTask(
        () -> {
          Optional<TsKvEntry> latest = doFindLatest(tenantId, entityId, telemetryKey);
          if (latest.isEmpty()) {
            return new TsKvLatestRemovingResult(telemetryKey, false);
          }
          long ts = latest.get().getTs();
          boolean removed = ts >= query.getStartTs() && ts < query.getEndTs();
          return new TsKvLatestRemovingResult(telemetryKey, removed, null);
        });
  }

  @Override
  public List<String> findAllKeysByDeviceProfileId(
      TenantId tenantId, DeviceProfileId deviceProfileId) {
    Objects.requireNonNull(tenantId, "tenantId");
    // Mirroring the reference SqlTimeseriesLatestDao: a null deviceProfileId is the "all profiles"
    // path and must return the tenant-wide distinct keys, which the telemetry table CAN derive.
    if (deviceProfileId == null) {
      try {
        return doFindAllKeysByTenant(tenantId);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read latest telemetry keys by tenant", e);
      }
    }
    // Non-null profile lookup stays deferred for a structural reason, not a missing implementation:
    // the IoTDB Table Mode telemetry table is tagged only by (tenant_id, entity_type, entity_id,
    // key) -- it carries NO device_profile_id column (see schema-iotdb-table.sql), so the
    // membership
    // "which devices belong to profile P" simply does not exist in this store. ThingsBoard's
    // relational backend answers this by JOINing the time-series keys against the relational device
    // table (device.device_profile_id), which lives in ThingsBoard's entity database, not in IoTDB.
    // Faking a tenant-wide or empty-but-pretending answer would silently widen or narrow attribute
    // discovery, so the honest behaviour is to return no keys for a specific profile and let the
    // caller's relational path own profile membership. (The null "all profiles" branch above is
    // fully derivable from the telemetry table and is implemented.)
    return Collections.emptyList();
  }

  @Override
  public List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityIds, "entityIds");
    // Synchronous SPI method: it runs on the calling thread (not the read executor), so the checked
    // session/query failure is surfaced to the caller as an unchecked exception.
    try {
      return doFindAllKeysByEntityIds(tenantId, entityIds);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read latest telemetry keys by entity ids", e);
    }
  }

  @Override
  public ListenableFuture<List<String>> findAllKeysByEntityIdsAsync(
      TenantId tenantId, List<EntityId> entityIds) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityIds, "entityIds");
    return submitReadTask(() -> doFindAllKeysByEntityIds(tenantId, entityIds));
  }

  @Override
  public void destroy() {
    if (!destroyed.compareAndSet(false, true)) {
      return;
    }
    accepting.set(false);
    IoTDBTableDaoShuttingDownException failure = shuttingDownException();
    for (Runnable dropped : readExecutor.shutdownNow()) {
      failDroppedReadTask(dropped, failure);
    }
    try {
      readExecutor.awaitTermination(shutdownDrainTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    for (ReadTask<?> task : readTasks) {
      task.fail(failure);
    }
  }

  private Optional<TsKvEntry> doFindLatest(TenantId tenantId, EntityId entityId, String key)
      throws Exception {
    String sql = buildFindLatestSql(tenantId, entityId, key);
    try (ITableSession session = tableSessionPool.getSession();
        SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      SessionDataSet.DataIterator row = dataSet.iterator();
      if (!row.next()) {
        return Optional.empty();
      }
      // B1 fail-fast: getEntry throws IllegalStateException if the selected latest row has more
      // than one typed column set (the mentor-approved Phase-1 same-timestamp type-change
      // limitation). The exception propagates so the future fails rather than returning bad data.
      TypedKvValue value = getEntry(row);
      if (!value.hasValue()) {
        return Optional.empty();
      }
      long ts = row.getTimestamp("time").getTime();
      return Optional.of(new BasicTsKvEntry(ts, kvEntry(key, value)));
    }
  }

  private List<TsKvEntry> doFindAllLatest(TenantId tenantId, EntityId entityId) throws Exception {
    String sql = buildFindAllLatestSql(tenantId, entityId);
    List<TsKvEntry> entries = new ArrayList<>();
    try (ITableSession session = tableSessionPool.getSession();
        SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      SessionDataSet.DataIterator row = dataSet.iterator();
      while (row.next()) {
        String key = row.getString("key");
        // LAST_BY(col, time) returns col at the row with the maximum time, preserving null, so the
        // aggregated columns form a single synthetic sparse row: exactly one typed column is
        // non-null for a clean key and no value is backfilled from an older different-type row.
        // B1 fail-fast: getEntry throws IllegalStateException if a key's aggregated row has more
        // than one non-null typed column; the whole findAllLatest future then fails rather than
        // silently skipping the bad key.
        TypedKvValue value = getEntry(row);
        if (!value.hasValue()) {
          continue;
        }
        long ts = row.getTimestamp("last_ts").getTime();
        entries.add(new BasicTsKvEntry(ts, kvEntry(key, value)));
      }
    }
    return entries;
  }

  private String buildFindLatestSql(TenantId tenantId, EntityId entityId, String key) {
    return "SELECT "
        + SELECT_TYPED_COLUMNS
        + " FROM "
        + TABLE_NAME
        + " WHERE tenant_id="
        + sqlString(tenantId.getId().toString())
        + " AND entity_type="
        + sqlString(entityId.getEntityType().name())
        + " AND entity_id="
        + sqlString(entityId.getId().toString())
        + " AND key="
        + sqlString(key)
        + " ORDER BY time DESC LIMIT 1";
  }

  private String buildFindAllLatestSql(TenantId tenantId, EntityId entityId) {
    // GROUP BY key projects the key tag plus the per-column LAST_BY aggregate (value at max time)
    // and MAX(time) for the entry timestamp. The other tags are fixed by the WHERE clause, so they
    // do not need to be (and cannot be) projected as bare columns alongside GROUP BY key.
    return "SELECT key,"
        + " LAST_BY(bool_v, time) AS bool_v,"
        + " LAST_BY(long_v, time) AS long_v,"
        + " LAST_BY(double_v, time) AS double_v,"
        + " LAST_BY(str_v, time) AS str_v,"
        + " LAST_BY(json_v, time) AS json_v,"
        + " MAX(time) AS last_ts"
        + " FROM "
        + TABLE_NAME
        + " WHERE tenant_id="
        + sqlString(tenantId.getId().toString())
        + " AND entity_type="
        + sqlString(entityId.getEntityType().name())
        + " AND entity_id="
        + sqlString(entityId.getId().toString())
        + " GROUP BY key";
  }

  private List<String> doFindAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds)
      throws Exception {
    if (entityIds.isEmpty()) {
      return List.of();
    }
    String sql = buildFindAllKeysByEntityIdsSql(tenantId, entityIds);
    List<String> keys = new ArrayList<>();
    try (ITableSession session = tableSessionPool.getSession();
        SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      SessionDataSet.DataIterator row = dataSet.iterator();
      while (row.next()) {
        keys.add(row.getString("key"));
      }
    }
    return keys;
  }

  private List<String> doFindAllKeysByTenant(TenantId tenantId) throws Exception {
    String sql =
        "SELECT DISTINCT key FROM "
            + TABLE_NAME
            + " WHERE tenant_id="
            + sqlString(tenantId.getId().toString());
    List<String> keys = new ArrayList<>();
    try (ITableSession session = tableSessionPool.getSession();
        SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      SessionDataSet.DataIterator row = dataSet.iterator();
      while (row.next()) {
        keys.add(row.getString("key"));
      }
    }
    return keys;
  }

  private String buildFindAllKeysByEntityIdsSql(TenantId tenantId, List<EntityId> entityIds) {
    StringBuilder sql =
        new StringBuilder("SELECT DISTINCT key FROM ")
            .append(TABLE_NAME)
            .append(" WHERE tenant_id=")
            .append(sqlString(tenantId.getId().toString()))
            .append(" AND (");
    for (int i = 0; i < entityIds.size(); i++) {
      EntityId entityId = Objects.requireNonNull(entityIds.get(i), "entityId");
      if (i > 0) {
        sql.append(" OR ");
      }
      sql.append("(entity_type=")
          .append(sqlString(entityId.getEntityType().name()))
          .append(" AND entity_id=")
          .append(sqlString(entityId.getId().toString()))
          .append(")");
    }
    sql.append(")");
    return sql.toString();
  }

  private static TsKvEntry nullEntry(String key) {
    // SPI contract: findLatest returns this sentinel when the value is not present in the DB.
    return new BasicTsKvEntry(System.currentTimeMillis(), new StringDataEntry(key, null));
  }

  private KvEntry kvEntry(String key, TypedKvValue value) {
    if (value.booleanValue() != null) {
      return new BooleanDataEntry(key, value.booleanValue());
    }
    if (value.longValue() != null) {
      return new LongDataEntry(key, value.longValue());
    }
    if (value.doubleValue() != null) {
      return new DoubleDataEntry(key, value.doubleValue());
    }
    if (value.stringValue() != null) {
      return new StringDataEntry(key, value.stringValue());
    }
    if (value.jsonValue() != null) {
      return new JsonDataEntry(key, value.jsonValue());
    }
    throw new IllegalArgumentException("Telemetry row does not contain a typed value");
  }

  private static String sqlString(String value) {
    return "'" + Objects.requireNonNull(value, "value").replace("'", "''") + "'";
  }

  private static String requireTelemetryKey(String key) {
    if (key == null || key.trim().isEmpty()) {
      throw new IllegalArgumentException("Telemetry key must not be blank");
    }
    return key;
  }

  private <T> ListenableFuture<T> submitReadTask(Callable<T> callable) {
    if (!accepting.get()) {
      return Futures.immediateFailedFuture(shuttingDownException());
    }
    ReadTask<T> task = new ReadTask<>(callable);
    readTasks.add(task);
    try {
      readExecutor.execute(task);
    } catch (RejectedExecutionException e) {
      if (!accepting.get() || readExecutor.isShutdown()) {
        task.fail(shuttingDownException());
      } else {
        task.fail(
            new IoTDBTableReadQueueFullException("IoTDB Table Mode latest read queue is full", e));
      }
      readTasks.remove(task);
      return task.future();
    }
    if (!accepting.get() && readExecutor.remove(task)) {
      task.fail(shuttingDownException());
      readTasks.remove(task);
    }
    return task.future();
  }

  private void failDroppedReadTask(Runnable dropped, IoTDBTableDaoShuttingDownException failure) {
    if (dropped instanceof ReadTask<?> task) {
      task.fail(failure);
      readTasks.remove(task);
    }
  }

  private IoTDBTableDaoShuttingDownException shuttingDownException() {
    return new IoTDBTableDaoShuttingDownException("IoTDB Table Mode latest DAO is shutting down");
  }

  private static ThreadFactory readThreadFactory() {
    AtomicInteger sequence = new AtomicInteger();
    return runnable -> {
      Thread thread =
          new Thread(runnable, "iotdb-table-latest-read-worker-" + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private final class ReadTask<T> implements Runnable {
    private final Callable<T> callable;
    private final SettableFuture<T> future = SettableFuture.create();

    private ReadTask(Callable<T> callable) {
      this.callable = Objects.requireNonNull(callable, "callable");
    }

    @Override
    public void run() {
      try {
        future.set(callable.call());
      } catch (Throwable t) {
        future.setException(t);
      } finally {
        readTasks.remove(this);
      }
    }

    private ListenableFuture<T> future() {
      return future;
    }

    private void fail(Throwable t) {
      future.setException(t);
    }
  }
}
