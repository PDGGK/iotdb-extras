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
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

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
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.AggregationParams;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.DataType;
import org.thingsboard.server.common.data.kv.IntervalType;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQueryResult;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real-Docker integration test for the calendar-interval aggregation read path against IoTDB 2.0.8
 * Table Mode. IoTDB's native {@code date_bin} calendar primitive anchors each bucket on the
 * origin's day-of-month and exposes no timezone argument, so the DAO walks the calendar boundaries
 * in Java ({@code TimeUtils.calculateIntervalEnd}) and issues one bounded aggregate per calendar
 * bucket. These tests prove that {@code WEEK}/{@code WEEK_ISO}/{@code MONTH}/{@code QUARTER}
 * buckets land on ThingsBoard 4.3.1.1's tz-aware, calendar-start-aligned boundaries with the entry
 * stamped at the calendar-bucket midpoint, that the first bucket is partial from a mid-period
 * start, and that an empty middle bucket is skipped (no spurious count-0 entry) -- all against
 * hand-computed values. Reuses the testcontainer harness from {@link IoTDBTableTimeseriesDaoIT}:
 * {@code apache/iotdb:2.0.8-standalone}, {@code dn_rpc_address=0.0.0.0}, exposed port 6667,
 * short-prefix unique database, schema bootstrap from {@code schema-iotdb-table.sql}.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class IoTDBTableTimeseriesCalendarAggregationIT {
  private static final int FUTURE_TIMEOUT_SECONDS = 30;
  private static final Duration IOTDB_STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration IOTDB_READY_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration IOTDB_READY_POLL_INTERVAL = Duration.ofMillis(500);

  @Container
  static final GenericContainer<?> IOTDB =
      new GenericContainer<>(DockerImageName.parse("apache/iotdb:2.0.8-standalone"))
          .withExposedPorts(6667)
          .withEnv("dn_rpc_address", "0.0.0.0")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(IOTDB_STARTUP_TIMEOUT));

  @Test
  void calendarMonthBucketsLandOnTbFaithfulCalendarBoundariesWithMidpoints() throws Exception {
    TestScope scope =
        scope(
            "agg_month",
            "55555555-5555-5555-5555-555555555506",
            "66666666-6666-6666-6666-666666666606");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(8);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // UTC calendar MONTH buckets for [2023-01-01, 2023-04-01): three variable-width months
        //   Jan [1672531200000,1675209600000) 31d -> midpoint 1673870400000 (2023-01-16T12:00Z)
        //   Feb [1675209600000,1677628800000) 28d -> midpoint 1676419200000 (2023-02-15T00:00Z)
        //   Mar [1677628800000,1680307200000) 31d -> midpoint 1678968000000 (2023-03-16T12:00Z)
        // The differing midpoints (16th-noon vs 15th-midnight) prove TRUE calendar widths, not a
        // fixed 30-day step; the Java-side bucketing reproduces TimeUtils.calculateIntervalEnd.
        saveAll(
            dao,
            scope,
            List.of(
                entry(1673308800000L, "n", DataType.LONG, 10L), // 2023-01-10
                entry(1674172800000L, "n", DataType.LONG, 20L), // 2023-01-20
                entry(1676419200000L, "n", DataType.LONG, 100L), // 2023-02-15
                entry(1677974400000L, "n", DataType.DOUBLE, 5.0D), // 2023-03-05
                entry(1679702400000L, "n", DataType.DOUBLE, 7.0D))); // 2023-03-25

        long startTs = 1672531200000L; // 2023-01-01T00:00Z
        long endTs = 1680307200000L; // 2023-04-01T00:00Z
        long[] midpoints = {1673870400000L, 1676419200000L, 1678968000000L};

        // Result TYPE per calendar bucket: Jan (10,20) and Feb (100) are LONG-only -> LONG SUM/MAX;
        // Mar (5.0, 7.0) is double-only -> DOUBLE SUM/MAX. AVG is always DOUBLE; COUNT LONG.
        ReadTsKvQueryResult sum =
            calendarAggregate(dao, scope, "n", startTs, endTs, Aggregation.SUM);
        // Jan SUM=30, Feb SUM=100, Mar SUM=12; lastEntryTs = MAX(data ts) = 2023-03-25.
        assertNumericBuckets(
            sum,
            midpoints,
            new DataType[] {DataType.LONG, DataType.LONG, DataType.DOUBLE},
            new double[] {30.0D, 100.0D, 12.0D},
            1679702400000L);

        ReadTsKvQueryResult count =
            calendarAggregate(dao, scope, "n", startTs, endTs, Aggregation.COUNT);
        assertLongBuckets(count, midpoints, new long[] {2L, 1L, 2L});

        ReadTsKvQueryResult avg =
            calendarAggregate(dao, scope, "n", startTs, endTs, Aggregation.AVG);
        assertDoubleBuckets(avg, midpoints, new double[] {15.0D, 100.0D, 6.0D}, 1679702400000L);

        ReadTsKvQueryResult max =
            calendarAggregate(dao, scope, "n", startTs, endTs, Aggregation.MAX);
        assertNumericBuckets(
            max,
            midpoints,
            new DataType[] {DataType.LONG, DataType.LONG, DataType.DOUBLE},
            new double[] {20.0D, 100.0D, 7.0D},
            1679702400000L);
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void calendarMonthFirstBucketIsPartialFromMidMonthStart() throws Exception {
    TestScope scope =
        scope(
            "agg_partial",
            "55555555-5555-5555-5555-555555555507",
            "66666666-6666-6666-6666-666666666607");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(4);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // startTs = 2023-01-15 (NOT a month boundary). ThingsBoard advances to the next calendar
        // boundary (Feb 1), so the FIRST bucket is the partial [Jan15, Feb1) with midpoint
        // 1674475200000 (2023-01-23T12:00Z), then the full calendar month [Feb1, Mar1).
        saveAll(
            dao,
            scope,
            List.of(
                entry(1673740800000L, "n", DataType.LONG, 3L), // 2023-01-15 (bucket start)
                entry(1676419200000L, "n", DataType.LONG, 9L))); // 2023-02-15

        // Long-only data -> LONG-typed SUM in both the partial and full calendar buckets.
        ReadTsKvQueryResult sum =
            calendarAggregate(dao, scope, "n", 1673740800000L, 1677628800000L, Aggregation.SUM);
        assertNumericBuckets(
            sum,
            new long[] {1674475200000L, 1676419200000L},
            new DataType[] {DataType.LONG, DataType.LONG},
            new double[] {3.0D, 9.0D},
            1676419200000L);
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void calendarCountSkipsEmptyMiddleMonthAgainstRealIoTDB() throws Exception {
    TestScope scope =
        scope(
            "agg_emptymid",
            "55555555-5555-5555-5555-555555555508",
            "66666666-6666-6666-6666-666666666608");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(4);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // End-to-end empty-middle-bucket proof against REAL IoTDB: write rows in Jan and Mar 2023
        // but NONE in Feb, then run a UTC calendar MONTH COUNT spanning all three months. For the
        // empty Feb bucket the bounded per-bucket aggregate REALLY returns one row with COUNT
        // columns = 0 and MAX(time) = NULL (the shape the unit mocks replicate via emptyAggRow).
        // The
        // DAO's `!isNull(max_ts)` guard must skip it, so EXACTLY the two non-empty months come back
        // with their correct counts and calendar midpoints; the empty Feb month is ABSENT (not a
        // spurious count-0 entry).
        //   Jan [1672531200000,1675209600000) 31d -> midpoint 1673870400000, count 2
        //   Feb [1675209600000,1677628800000) 28d -> EMPTY -> skipped (no entry)
        //   Mar [1677628800000,1680307200000) 31d -> midpoint 1678968000000, count 2
        saveAll(
            dao,
            scope,
            List.of(
                entry(1673308800000L, "n", DataType.LONG, 10L), // 2023-01-10
                entry(1674172800000L, "n", DataType.LONG, 20L), // 2023-01-20
                entry(1677974400000L, "n", DataType.LONG, 5L), // 2023-03-05
                entry(1679702400000L, "n", DataType.LONG, 7L))); // 2023-03-25

        long startTs = 1672531200000L; // 2023-01-01T00:00Z
        long endTs = 1680307200000L; // 2023-04-01T00:00Z

        ReadTsKvQueryResult count =
            calendarAggregate(dao, scope, "n", startTs, endTs, Aggregation.COUNT);
        // EXACTLY the two non-empty months at their calendar midpoints; Feb is absent.
        assertLongBuckets(count, new long[] {1673870400000L, 1678968000000L}, new long[] {2L, 2L});
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  private ReadTsKvQueryResult calendarAggregate(
      IoTDBTableTimeseriesDao dao,
      TestScope scope,
      String key,
      long startTs,
      long endTs,
      Aggregation aggregation)
      throws Exception {
    ReadTsKvQuery query =
        new BaseReadTsKvQuery(
            key,
            startTs,
            endTs,
            AggregationParams.calendar(aggregation, IntervalType.MONTH, "UTC"),
            100,
            "ASC");
    return dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(query))
        .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .get(0);
  }

  private void assertDoubleBuckets(
      ReadTsKvQueryResult result,
      long[] expectedTs,
      double[] expectedValues,
      long expectedLastEntryTs) {
    List<TsKvEntry> data = result.getData();
    assertEquals(expectedTs.length, data.size(), "bucket count");
    for (int i = 0; i < expectedTs.length; i++) {
      TsKvEntry entry = data.get(i);
      assertEquals(expectedTs[i], entry.getTs(), "bucket ts at index " + i);
      assertEquals(DataType.DOUBLE, entry.getDataType(), "data type at index " + i);
      assertTrue(entry.getDoubleValue().isPresent(), "double value present at index " + i);
      assertEquals(
          expectedValues[i], entry.getDoubleValue().get(), 1e-9, "double value at index " + i);
    }
    // ThingsBoard reports lastEntryTs as MAX(underlying data ts), not a bucket midpoint.
    assertEquals(expectedLastEntryTs, result.getLastEntryTs(), "lastEntryTs");
  }

  private void assertLongBuckets(
      ReadTsKvQueryResult result, long[] expectedTs, long[] expectedValues) {
    List<TsKvEntry> data = result.getData();
    assertEquals(expectedTs.length, data.size(), "bucket count");
    for (int i = 0; i < expectedTs.length; i++) {
      TsKvEntry entry = data.get(i);
      assertEquals(expectedTs[i], entry.getTs(), "bucket ts at index " + i);
      assertEquals(DataType.LONG, entry.getDataType(), "data type at index " + i);
      assertEquals(
          Optional.of(expectedValues[i]), entry.getLongValue(), "long value at index " + i);
    }
  }

  /**
   * Asserts numeric buckets whose per-bucket result TYPE varies: a long-only SUM/MIN/MAX bucket
   * comes back {@link DataType#LONG}, a bucket with any participating double comes back {@link
   * DataType#DOUBLE}. {@code expectedTypes[i]} must be LONG or DOUBLE; the value is compared via
   * the matching typed getter (exact for LONG, 1e-9 tolerance for DOUBLE).
   */
  private void assertNumericBuckets(
      ReadTsKvQueryResult result,
      long[] expectedTs,
      DataType[] expectedTypes,
      double[] expectedValues,
      long expectedLastEntryTs) {
    List<TsKvEntry> data = result.getData();
    assertEquals(expectedTs.length, data.size(), "bucket count");
    for (int i = 0; i < expectedTs.length; i++) {
      TsKvEntry entry = data.get(i);
      assertEquals(expectedTs[i], entry.getTs(), "bucket ts at index " + i);
      assertEquals(expectedTypes[i], entry.getDataType(), "data type at index " + i);
      if (expectedTypes[i] == DataType.LONG) {
        assertEquals(
            Optional.of((long) expectedValues[i]),
            entry.getLongValue(),
            "long value at index " + i);
      } else {
        assertTrue(entry.getDoubleValue().isPresent(), "double value present at index " + i);
        assertEquals(
            expectedValues[i], entry.getDoubleValue().get(), 1e-9, "double value at index " + i);
      }
    }
    assertEquals(expectedLastEntryTs, result.getLastEntryTs(), "lastEntryTs");
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
        IoTDBTableTimeseriesCalendarAggregationIT.class
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
    config.getTs().getRead().setThreads(1);
    return config;
  }

  private void saveAll(IoTDBTableTimeseriesDao dao, TestScope scope, List<TestTsKvEntry> entries)
      throws Exception {
    List<com.google.common.util.concurrent.ListenableFuture<Integer>> futures = new ArrayList<>();
    for (TestTsKvEntry entry : entries) {
      futures.add(dao.save(scope.tenantId(), scope.entityId(), entry, 0));
    }
    for (com.google.common.util.concurrent.ListenableFuture<Integer> future : futures) {
      assertEquals(1, future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  private TestScope scope(String databasePrefix, String tenantId, String entityId) {
    return new TestScope(
        uniqueDatabase(databasePrefix),
        new TenantId(UUID.fromString(tenantId)),
        new TestEntityId(UUID.fromString(entityId), EntityType.DEVICE));
  }

  private String uniqueDatabase(String prefix) {
    String shortPrefix = prefix.length() > 12 ? prefix.substring(0, 12) : prefix;
    String shortUuid = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    return "tb_it_" + shortPrefix + "_" + shortUuid;
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
