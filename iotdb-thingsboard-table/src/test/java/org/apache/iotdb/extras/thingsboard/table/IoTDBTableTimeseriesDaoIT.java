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
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.DataType;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GSOC-304 Wk 2 integration tests for the IoTDB Table Mode timeseries WRITE path against a real
 * IoTDB 2.0.8 container. Writes are verified by reading the telemetry table back through raw
 * table-session SQL (the DAO read path is introduced in a later week).
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class IoTDBTableTimeseriesDaoIT {
  // Cold testcontainer first writes are slower than a warm production node, so the per-future
  // assertion timeout is generous; production throughput is covered elsewhere, not here.
  private static final int FUTURE_TIMEOUT_SECONDS = 30;
  private static final Duration IOTDB_STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration IOTDB_READY_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration IOTDB_READY_POLL_INTERVAL = Duration.ofMillis(500);

  @Container
  static final GenericContainer<?> IOTDB =
      new GenericContainer<>(DockerImageName.parse("apache/iotdb:2.0.8-standalone"))
          .withExposedPorts(6667)
          // IoTDB binds its client RPC service to dn_rpc_address (default 127.0.0.1), so it would
          // only listen on the container loopback and reject the Testcontainers port-mapped session
          // handshake ("Fail to reconnect"). Bind to all interfaces so the mapped host port works.
          .withEnv("dn_rpc_address", "0.0.0.0")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(IOTDB_STARTUP_TIMEOUT));

  @Test
  void schemaBootstrap_writesAllTypesAndReadsExactlyOneField() throws Exception {
    TestScope scope =
        scope(
            "all_types",
            "33333333-3333-3333-3333-333333333301",
            "44444444-4444-4444-4444-444444444401");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(5);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        List.of(
                dao.save(
                    scope.tenantId(),
                    scope.entityId(),
                    entry(1000L, "bool", DataType.BOOLEAN, true),
                    0),
                dao.save(
                    scope.tenantId(),
                    scope.entityId(),
                    entry(1001L, "long", DataType.LONG, 42L),
                    0),
                dao.save(
                    scope.tenantId(),
                    scope.entityId(),
                    entry(1002L, "double", DataType.DOUBLE, 4.2D),
                    0),
                dao.save(
                    scope.tenantId(),
                    scope.entityId(),
                    entry(1003L, "string", DataType.STRING, "value"),
                    0),
                dao.save(
                    scope.tenantId(),
                    scope.entityId(),
                    entry(1004L, "json", DataType.JSON, "{\"v\":1}"),
                    0))
            .forEach(
                future -> {
                  try {
                    assertEquals(1, future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                  } catch (Exception e) {
                    throw new AssertionError(e);
                  }
                });

        int rowCount = 0;
        for (String key : List.of("bool", "long", "double", "string", "json")) {
          rowCount += assertTelemetryRows(pool, scope, key, 1, 1);
        }
        assertEquals(5, rowCount);
      } finally {
        writer.destroy();
      }
    }
  }

  @Test
  void writesFiveHundredMixedEntriesInOneFlush() throws Exception {
    TestScope scope =
        scope(
            "mixed_batch",
            "33333333-3333-3333-3333-333333333302",
            "44444444-4444-4444-4444-444444444402");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(500);
      config.getTs().getSave().setMaxLingerMs(5000L);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        List<ListenableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
          futures.add(
              dao.save(
                  scope.tenantId(),
                  scope.entityId(),
                  entry(2000L + i, "mixed-" + i, DataType.LONG, (long) i),
                  0));
        }
        for (ListenableFuture<Integer> future : futures) {
          assertEquals(1, future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        assertEquals(500, dao.stats().flushed());
        assertTelemetryRows(pool, scope, "mixed-499", 1, 1);
      } finally {
        writer.destroy();
      }
    }
  }

  private ITableSessionPool newPool(String database) {
    TableSessionPoolBuilder builder =
        new TableSessionPoolBuilder()
            .nodeUrls(List.of("127.0.0.1:" + IOTDB.getMappedPort(6667)))
            .user("root")
            .password("root")
            .maxSize(4);
    if (database != null) {
      builder.database(database);
    }
    return builder.build();
  }

  private void bootstrapSchema(String database) throws Exception {
    awaitIoTDBReady(database);

    String schema;
    try (InputStream stream =
        IoTDBTableTimeseriesDaoIT.class
            .getClassLoader()
            .getResourceAsStream("schema-iotdb-table.sql")) {
      schema = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
    schema =
        schema
            .replace(
                "CREATE DATABASE IF NOT EXISTS thingsboard;",
                "CREATE DATABASE IF NOT EXISTS " + database + ";")
            .replace("USE thingsboard;", "USE " + database + ";");
    schema = schema.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)--.*$", "");
    try (ITableSessionPool bootstrapPool = newPool(null)) {
      try (ITableSession session = bootstrapPool.getSession()) {
        for (String statement : schema.split(";")) {
          String trimmed = statement.trim();
          if (!trimmed.isEmpty()) {
            session.executeNonQueryStatement(trimmed);
          }
        }
      }
    }
  }

  private void awaitIoTDBReady(String database) throws Exception {
    long deadlineNanos = System.nanoTime() + IOTDB_READY_TIMEOUT.toNanos();
    Exception lastFailure = null;
    while (System.nanoTime() < deadlineNanos) {
      try (ITableSessionPool bootstrapPool = newPool(null);
          ITableSession session = bootstrapPool.getSession()) {
        session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + database);
        return;
      } catch (Exception e) {
        lastFailure = e;
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (remainingMillis <= 0) {
          break;
        }
        Thread.sleep(Math.min(IOTDB_READY_POLL_INTERVAL.toMillis(), remainingMillis));
      }
    }
    throw new IllegalStateException(
        "IoTDB did not accept table-session statements within " + IOTDB_READY_TIMEOUT, lastFailure);
  }

  private IoTDBTableConfig config(int batchSize) {
    IoTDBTableConfig config = new IoTDBTableConfig();
    config.getTs().getSave().setBatchSize(batchSize);
    config.getTs().getSave().setMaxLingerMs(20L);
    config.getTs().getSave().setRetryInitialBackoffMs(1L);
    config.getTs().getSave().setRetryMaxBackoffMs(1L);
    return config;
  }

  private TestScope scope(String databasePrefix, String tenantId, String entityId) {
    return new TestScope(
        uniqueDatabase(databasePrefix),
        new TenantId(UUID.fromString(tenantId)),
        new TestEntityId(UUID.fromString(entityId), EntityType.DEVICE));
  }

  private String uniqueDatabase(String prefix) {
    // IoTDB caps database names at 64 chars; keep the per-test prefix short and
    // append a trimmed UUID so the total length stays well within the limit.
    String shortPrefix = prefix.length() > 12 ? prefix.substring(0, 12) : prefix;
    String shortUuid = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    return "tb_it_" + shortPrefix + "_" + shortUuid;
  }

  private int assertTelemetryRows(
      ITableSessionPool pool,
      TestScope scope,
      String key,
      int expectedRows,
      int expectedTypedFields)
      throws Exception {
    try (ITableSession session = pool.getSession();
        SessionDataSet dataSet =
            session.executeQueryStatement(
                "SELECT bool_v,long_v,double_v,str_v,json_v FROM telemetry "
                    + telemetryWhere(scope, key))) {
      SessionDataSet.DataIterator rows = dataSet.iterator();
      int rowCount = 0;
      while (rows.next()) {
        assertEquals(expectedTypedFields, typedFieldCount(rows));
        rowCount++;
      }
      assertEquals(expectedRows, rowCount);
      return rowCount;
    }
  }

  private String telemetryWhere(TestScope scope, String key) {
    return "WHERE tenant_id='"
        + scope.tenantId().getId()
        + "' AND entity_type='DEVICE' AND entity_id='"
        + scope.entityId().getId()
        + "' AND key='"
        + key
        + "'";
  }

  private int typedFieldCount(SessionDataSet.DataIterator row) throws Exception {
    int count = 0;
    count += row.isNull("bool_v") ? 0 : 1;
    count += row.isNull("long_v") ? 0 : 1;
    count += row.isNull("double_v") ? 0 : 1;
    count += row.isNull("str_v") ? 0 : 1;
    count += row.isNull("json_v") ? 0 : 1;
    return count;
  }

  private TestTsKvEntry entry(long ts, String key, DataType dataType, Object value) {
    return new TestTsKvEntry(ts, key, dataType, value);
  }

  private record TestScope(String database, TenantId tenantId, EntityId entityId) {}

  private record TestEntityId(UUID id, EntityType entityType) implements EntityId {
    @Override
    public UUID getId() {
      return id;
    }

    @Override
    public EntityType getEntityType() {
      return entityType;
    }
  }

  private record TestTsKvEntry(long ts, String key, DataType dataType, Object value)
      implements TsKvEntry {
    @Override
    public long getTs() {
      return ts;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public DataType getDataType() {
      return dataType;
    }

    @Override
    public Optional<Boolean> getBooleanValue() {
      return dataType == DataType.BOOLEAN ? Optional.of((Boolean) value) : Optional.empty();
    }

    @Override
    public Optional<Long> getLongValue() {
      return dataType == DataType.LONG ? Optional.of((Long) value) : Optional.empty();
    }

    @Override
    public Optional<Double> getDoubleValue() {
      return dataType == DataType.DOUBLE ? Optional.of((Double) value) : Optional.empty();
    }

    @Override
    public Optional<String> getStrValue() {
      return dataType == DataType.STRING ? Optional.of((String) value) : Optional.empty();
    }

    @Override
    public Optional<String> getJsonValue() {
      return dataType == DataType.JSON ? Optional.of((String) value) : Optional.empty();
    }

    @Override
    public String getValueAsString() {
      return String.valueOf(value);
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public Long getVersion() {
      return null;
    }

    @Override
    public int getDataPoints() {
      return 1;
    }
  }
}
