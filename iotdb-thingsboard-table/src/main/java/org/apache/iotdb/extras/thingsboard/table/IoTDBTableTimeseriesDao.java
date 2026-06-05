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

import org.apache.iotdb.isession.pool.ITableSessionPool;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.DataType;
import org.thingsboard.server.common.data.kv.DeleteTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQueryResult;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Historical telemetry DAO for the IoTDB Table Mode backend.
 *
 * <p>Spring activation: database.ts.type=iotdb-table.
 *
 * <p>Strategy F consumes ThingsBoard common-data types from the compile classpath and binds the
 * real historical {@link TimeseriesDao} SPI.
 *
 * <p>GSOC-304 Wk 2 delivers the batch WRITE path only ({@link #save}); the read, delete and
 * aggregation methods of the SPI are introduced in later weeks and are not yet implemented here.
 *
 * @see "GSOC-304 design doc section 6.2"
 * @since GSOC-304 Wk 1 scaffold
 */
@Slf4j
@Repository
@ConditionalOnBean(ITableSessionPool.class)
@ConditionalOnProperty(name = "database.ts.type", havingValue = "iotdb-table")
public class IoTDBTableTimeseriesDao extends IoTDBTableBaseDao
    implements TimeseriesDao, DisposableBean {
  private static final long SECONDS_PER_DAY = 86400L;

  private final IoTDBTableTimeseriesWriter timeseriesWriter;
  private final AtomicBoolean destroyed = new AtomicBoolean(false);
  private final long defaultTtlSeconds;

  public IoTDBTableTimeseriesDao(
      ITableSessionPool tableSessionPool,
      IoTDBTableTimeseriesWriter timeseriesWriter,
      IoTDBTableConfig config) {
    super(tableSessionPool);
    this.timeseriesWriter = Objects.requireNonNull(timeseriesWriter, "timeseriesWriter");
    this.defaultTtlSeconds =
        config.getDefaultTtlMs() > 0L
            ? TimeUnit.MILLISECONDS.toSeconds(config.getDefaultTtlMs())
            : 0L;
  }

  @Override
  public ListenableFuture<Integer> save(
      TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(entityId, "entityId");
    Objects.requireNonNull(tsKvEntry, "tsKvEntry");

    try {
      String key = requireTelemetryKey(tsKvEntry.getKey());
      return timeseriesWriter.enqueue(
          new IoTDBTablePendingSave(
              tenantId.getId().toString(),
              entityId.getEntityType().name(),
              entityId.getId().toString(),
              key,
              tsKvEntry.getTs(),
              tsKvEntry.getDataType(),
              typedValue(tsKvEntry),
              dataPointDays(tsKvEntry, ttl)));
    } catch (RuntimeException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public ListenableFuture<Integer> savePartition(
      TenantId tenantId, EntityId entityId, long ts, String key) {
    // IoTDB Table Mode has no per-partition bookkeeping; the write path is partition-agnostic, so
    // there is nothing to persist for a partition marker. Matches the contract ThingsBoard expects
    // from a DAO that does not maintain a partitions table.
    return Futures.immediateFuture(0);
  }

  @Override
  public ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(
      TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries) {
    // GSOC-304 Wk 3: historical read path (raw + aggregation) is not yet implemented.
    throw new UnsupportedOperationException(
        "IoTDB Table Mode timeseries read path is not implemented yet (GSOC-304 Wk 3)");
  }

  @Override
  public ListenableFuture<Void> remove(
      TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
    // GSOC-304 Wk 3: telemetry delete path is not yet implemented.
    throw new UnsupportedOperationException(
        "IoTDB Table Mode timeseries delete path is not implemented yet (GSOC-304 Wk 3)");
  }

  @Override
  public void cleanup(long systemTtl) {
    // No-op: physical retention is a table-level IoTDB property (TTL in ms), owned by the
    // operator's schema (WITH (TTL=<ms>) or ALTER TABLE telemetry SET PROPERTIES TTL=<ms>), not
    // driven from this per-call hook. IoTDB Table Mode TTL cannot honor a per-data-point ttl, so
    // the module does not issue retention DDL here.
  }

  public IoTDBTableTimeseriesWriterStats stats() {
    return timeseriesWriter.stats();
  }

  @Override
  public void destroy() {
    if (!destroyed.compareAndSet(false, true)) {
      return;
    }
    timeseriesWriter.destroy();
  }

  private static String requireTelemetryKey(String key) {
    if (key == null || key.trim().isEmpty()) {
      throw new IllegalArgumentException("Telemetry key must not be blank");
    }
    return key;
  }

  private int dataPointDays(TsKvEntry tsKvEntry, long ttl) {
    long effectiveTtlSeconds =
        ttl <= 0L
            ? defaultTtlSeconds
            : (defaultTtlSeconds > 0L ? Math.min(defaultTtlSeconds, ttl) : ttl);
    long ttlDays = Math.max(1L, effectiveTtlSeconds / SECONDS_PER_DAY);
    return Math.toIntExact((long) tsKvEntry.getDataPoints() * ttlDays);
  }

  private Object typedValue(TsKvEntry tsKvEntry) {
    DataType dataType = tsKvEntry.getDataType();
    return switch (dataType) {
      case BOOLEAN -> requiredValue(tsKvEntry.getBooleanValue(), dataType);
      case LONG -> requiredValue(tsKvEntry.getLongValue(), dataType);
      case DOUBLE -> requiredValue(tsKvEntry.getDoubleValue(), dataType);
      case STRING -> requiredValue(tsKvEntry.getStrValue(), dataType);
      case JSON -> requiredValue(tsKvEntry.getJsonValue(), dataType);
    };
  }

  private Object requiredValue(Optional<?> value, DataType dataType) {
    return value.orElseThrow(
        () -> new IllegalArgumentException("Missing value for telemetry data type " + dataType));
  }
}
