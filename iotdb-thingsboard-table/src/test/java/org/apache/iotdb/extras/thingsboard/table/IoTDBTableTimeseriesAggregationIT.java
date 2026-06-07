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
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.DataType;
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
 * Real-Docker integration test that proves the IoTDB 2.0.8 Table Mode native three-argument {@code
 * date_bin(<interval>ms, time, <startTs>)} + {@code GROUP BY} time-bucketed aggregation path
 * matches ThingsBoard 4.3.1.1's contract: buckets anchored at {@code startTs} (not epoch 1970),
 * entries stamped at the bucket midpoint, every non-empty bucket returned ascending regardless of
 * query order/limit, and typed COUNT semantics -- all against hand-computed expected values. Reuses
 * the testcontainer harness from {@link IoTDBTableTimeseriesDaoIT}: {@code
 * apache/iotdb:2.0.8-standalone}, {@code dn_rpc_address=0.0.0.0}, exposed port 6667, short-prefix
 * unique database, schema bootstrap from {@code schema-iotdb-table.sql}.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class IoTDBTableTimeseriesAggregationIT {
  private static final int FUTURE_TIMEOUT_SECONDS = 30;
  private static final Duration IOTDB_STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration IOTDB_READY_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration IOTDB_READY_POLL_INTERVAL = Duration.ofMillis(500);
  private static final long INTERVAL = 1000L;

  @Container
  static final GenericContainer<?> IOTDB =
      new GenericContainer<>(DockerImageName.parse("apache/iotdb:2.0.8-standalone"))
          .withExposedPorts(6667)
          .withEnv("dn_rpc_address", "0.0.0.0")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(IOTDB_STARTUP_TIMEOUT));

  @Test
  void numericAggregationsBucketCorrectlyAgainstHandComputedValues() throws Exception {
    TestScope scope =
        scope(
            "agg_numeric",
            "55555555-5555-5555-5555-555555555501",
            "66666666-6666-6666-6666-666666666601");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(8);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // Query [1000,3000), interval 1000, startTs-anchored buckets (origin=1000):
        //   Bucket [1000,2000): 10, 20, 5.5 -> midpoint 1500; sum 35.5, count 3, avg 11.8333,
        //       min 5.5, max 20.0
        //   Bucket [2000,3000): 30, 40      -> midpoint 2500; sum 70.0, count 2, avg 35.0,
        //       min 30.0, max 40.0
        //   Bucket [3000,4000): empty       -> no entry emitted
        // lastEntryTs = MAX(underlying time) = 2700 (ThingsBoard reports the max data ts).
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "n", DataType.LONG, 10L),
                entry(1200L, "n", DataType.LONG, 20L),
                entry(1500L, "n", DataType.DOUBLE, 5.5D),
                entry(2100L, "n", DataType.LONG, 30L),
                entry(2700L, "n", DataType.LONG, 40L)));

        // Per-bucket result type: bucket [1000,2000) is MIXED (has the 5.5 double) so SUM/MIN/MAX
        // stay DOUBLE; bucket [2000,3000) is LONG-ONLY (30, 40) so SUM/MIN/MAX come back LONG-typed
        // (TB 4.3.1.1 keeps a long-only SUM/MIN/MAX LONG). AVG is always DOUBLE; COUNT is always
        // LONG.
        ReadTsKvQueryResult avg = aggregate(dao, scope, "n", Aggregation.AVG);
        assertDoubleBuckets(
            avg, new long[] {1500L, 2500L}, new double[] {35.5D / 3D, 35.0D}, 2700L);

        ReadTsKvQueryResult sum = aggregate(dao, scope, "n", Aggregation.SUM);
        assertNumericBuckets(
            sum,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.DOUBLE, DataType.LONG},
            new double[] {35.5D, 70.0D},
            2700L);

        ReadTsKvQueryResult count = aggregate(dao, scope, "n", Aggregation.COUNT);
        assertLongBuckets(count, new long[] {1500L, 2500L}, new long[] {3L, 2L});

        ReadTsKvQueryResult min = aggregate(dao, scope, "n", Aggregation.MIN);
        assertNumericBuckets(
            min,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.DOUBLE, DataType.LONG},
            new double[] {5.5D, 30.0D},
            2700L);

        ReadTsKvQueryResult max = aggregate(dao, scope, "n", Aggregation.MAX);
        assertNumericBuckets(
            max,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.DOUBLE, DataType.LONG},
            new double[] {20.0D, 40.0D},
            2700L);
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void longOnlyAndMixedBucketsKeepThingsBoardResultTypeAgainstRealIoTDB() throws Exception {
    TestScope scope =
        scope(
            "agg_resulttype",
            "55555555-5555-5555-5555-555555555509",
            "66666666-6666-6666-6666-666666666609");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(8);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // End-to-end proof of the result-type contract against REAL IoTDB:
        //   Bucket [1000,2000): LONG-ONLY data 4,6,10 -> midpoint 1500;
        //       sum 20, min 4, max 10 -- all LONG-typed (TB 4.3.1.1 keeps a long-only result LONG).
        //   Bucket [2000,3000): MIXED data 3 (long) + 2.5 (double) -> midpoint 2500;
        //       sum 5.5, min 2.5, max 3.0 -- all DOUBLE-typed (a participating double promotes).
        // AVG is always DOUBLE; COUNT is always LONG.
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "m", DataType.LONG, 4L),
                entry(1300L, "m", DataType.LONG, 6L),
                entry(1700L, "m", DataType.LONG, 10L),
                entry(2100L, "m", DataType.LONG, 3L),
                entry(2400L, "m", DataType.DOUBLE, 2.5D)));

        ReadTsKvQueryResult sum = aggregate(dao, scope, "m", Aggregation.SUM);
        assertNumericBuckets(
            sum,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.LONG, DataType.DOUBLE},
            new double[] {20.0D, 5.5D},
            2400L);

        ReadTsKvQueryResult min = aggregate(dao, scope, "m", Aggregation.MIN);
        assertNumericBuckets(
            min,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.LONG, DataType.DOUBLE},
            new double[] {4.0D, 2.5D},
            2400L);

        ReadTsKvQueryResult max = aggregate(dao, scope, "m", Aggregation.MAX);
        assertNumericBuckets(
            max,
            new long[] {1500L, 2500L},
            new DataType[] {DataType.LONG, DataType.DOUBLE},
            new double[] {10.0D, 3.0D},
            2400L);

        // AVG stays DOUBLE even for the long-only bucket; COUNT stays LONG everywhere.
        ReadTsKvQueryResult avg = aggregate(dao, scope, "m", Aggregation.AVG);
        assertDoubleBuckets(
            avg, new long[] {1500L, 2500L}, new double[] {20.0D / 3D, 2.75D}, 2400L);

        ReadTsKvQueryResult count = aggregate(dao, scope, "m", Aggregation.COUNT);
        assertLongBuckets(count, new long[] {1500L, 2500L}, new long[] {3L, 2L});
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void sumLongOnlyExceedingLongMaxDoesNotCrashAndReSumsExactlyAgainstRealIoTDB() throws Exception {
    TestScope scope =
        scope(
            "agg_overflow",
            "55555555-5555-5555-5555-555555555511",
            "66666666-6666-6666-6666-666666666611");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(4);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // End-to-end proof the unsafe INT64-cast path is GONE. Two longs near Long.MAX land in ONE
        // long-only bucket [1000,2000); their true sum 18446744073709550000 exceeds Long.MAX
        // (9223372036854775807). Against REAL IoTDB the old projection CAST(SUM(long_v) AS INT64)
        // THROWS "Double value ... out of range of long value" and FAILS the whole aggregate query
        // before the Java bound-check/fallback can run. The new SUM(CAST(long_v AS DOUBLE)) partial
        // never throws, the bound (count_long=2, maxAbs ~ 9.2e18 -> 2 > 2^53/maxAbs) forces the
        // exact Java re-sum, and the DAO returns the bucket with the EXACT long value -- the same
        // natural 2^64 overflow ThingsBoard's long arithmetic produces: -1616.
        long nearMax = 9223372036854775000L; // sum of two exceeds Long.MAX
        long overflowSum = nearMax + nearMax; // -1616 under Java natural long overflow (== TB)
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "o", DataType.LONG, nearMax),
                entry(1500L, "o", DataType.LONG, nearMax)));

        ReadTsKvQuery query =
            new BaseReadTsKvQuery("o", 1000L, 2000L, INTERVAL, 100, Aggregation.SUM, "ASC");
        ReadTsKvQueryResult sum =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(query))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);

        // The query did NOT crash (it returned), and the long-only bucket comes back at midpoint
        // 1500
        // with the EXACT Java re-sum (-1616), proving the fallback handled the > Long.MAX sum.
        assertEquals(1, sum.getData().size(), "exactly one long-only bucket returned (no crash)");
        assertExactNumericBuckets(
            sum,
            new long[] {1500L},
            new DataType[] {DataType.LONG},
            new long[] {overflowSum},
            new double[] {0D},
            1500L);
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void stringMinMaxBucketLexicographically() throws Exception {
    TestScope scope =
        scope(
            "agg_string",
            "55555555-5555-5555-5555-555555555502",
            "66666666-6666-6666-6666-666666666602");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(4);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // startTs-anchored buckets (origin=1000), midpoints 1500 / 2500:
        //   Bucket [1000,2000): 'banana','apple' -> midpoint 1500; min 'apple', max 'banana'
        //   Bucket [2000,3000): 'cherry'         -> midpoint 2500; min/max 'cherry'
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "s", DataType.STRING, "banana"),
                entry(1200L, "s", DataType.STRING, "apple"),
                entry(2100L, "s", DataType.STRING, "cherry")));

        ReadTsKvQueryResult min = aggregate(dao, scope, "s", Aggregation.MIN);
        assertStringBuckets(min, new long[] {1500L, 2500L}, new String[] {"apple", "cherry"});

        ReadTsKvQueryResult max = aggregate(dao, scope, "s", Aggregation.MAX);
        assertStringBuckets(max, new long[] {1500L, 2500L}, new String[] {"banana", "cherry"});
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void aggregationIgnoresLimitOrderAndReturnsAllBucketsAscending() throws Exception {
    TestScope scope =
        scope(
            "agg_limit",
            "55555555-5555-5555-5555-555555555503",
            "66666666-6666-6666-6666-666666666603");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(6);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // Query [1000,5000), interval 1000, startTs-anchored buckets (origin=1000):
        //   [1000,2000) data 1000 -> midpoint 1500
        //   [2000,3000) data 2000 -> midpoint 2500
        //   [3000,4000) empty     -> skipped
        //   [4000,5000) data 4000 -> midpoint 4500
        // ThingsBoard ignores query order/limit for aggregation: every non-empty bucket is
        // returned in ascending time order regardless of LIMIT 2 / ASC-vs-DESC.
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "n", DataType.LONG, 1L),
                entry(2000L, "n", DataType.LONG, 2L),
                entry(4000L, "n", DataType.LONG, 4L)));

        long[] expectedTs = {1500L, 2500L, 4500L};
        // Long-only data -> the SUM result is LONG-typed in every bucket.
        DataType[] expectedTypes = {DataType.LONG, DataType.LONG, DataType.LONG};
        double[] expectedValues = {1.0D, 2.0D, 4.0D};
        long expectedLastTs = 4000L; // MAX(underlying time)

        // ASC + LIMIT 2: limit and order are ignored; all three buckets come back ascending.
        ReadTsKvQuery asc =
            new BaseReadTsKvQuery("n", 1000L, 5000L, INTERVAL, 2, Aggregation.SUM, "ASC");
        ReadTsKvQueryResult ascResult =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(asc))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);
        assertNumericBuckets(ascResult, expectedTs, expectedTypes, expectedValues, expectedLastTs);

        // DESC + LIMIT 2: identical result -- order and limit have no effect on aggregation.
        ReadTsKvQuery desc =
            new BaseReadTsKvQuery("n", 1000L, 5000L, INTERVAL, 2, Aggregation.SUM, "DESC");
        ReadTsKvQueryResult descResult =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(desc))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);
        assertNumericBuckets(descResult, expectedTs, expectedTypes, expectedValues, expectedLastTs);
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void nonAlignedStartTsBucketsAnchorAtStartTsNotEpochZero() throws Exception {
    TestScope scope =
        scope(
            "agg_nonalign",
            "55555555-5555-5555-5555-555555555505",
            "66666666-6666-6666-6666-666666666605");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(4);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        // startTs=1001 is NOT a multiple of interval=1000. With epoch-0 (2-arg date_bin) the point
        // at 2000 would land in bucket [2000,3000); with startTs-anchored buckets (3-arg origin)
        // it lands in [1001,2001) -> midpoint 1001 + (2001-1001)/2 = 1501. A second point at 2500
        // lands in [2001,3001) -> midpoint 2501. This proves epoch-0 misalignment is gone.
        saveAll(
            dao,
            scope,
            List.of(entry(2000L, "n", DataType.LONG, 7L), entry(2500L, "n", DataType.LONG, 9L)));

        ReadTsKvQuery query =
            new BaseReadTsKvQuery("n", 1001L, 3001L, INTERVAL, 100, Aggregation.SUM, "ASC");
        ReadTsKvQueryResult result =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(query))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);

        // Bucket [1001,2001) midpoint 1501 holds 7; bucket [2001,3001) midpoint 2501 holds 9.
        // Long-only data -> LONG-typed SUM in both buckets.
        assertNumericBuckets(
            result,
            new long[] {1501L, 2501L},
            new DataType[] {DataType.LONG, DataType.LONG},
            new double[] {7.0D, 9.0D},
            2500L);

        // COUNT on the same non-aligned range yields the same midpoints and counts of 1 each.
        ReadTsKvQuery countQuery =
            new BaseReadTsKvQuery("n", 1001L, 3001L, INTERVAL, 100, Aggregation.COUNT, "ASC");
        ReadTsKvQueryResult countResult =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(countQuery))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);
        assertLongBuckets(countResult, new long[] {1501L, 2501L}, new long[] {1L, 1L});
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  @Test
  void rawPathStillWorksWhenAggregationIsNone() throws Exception {
    TestScope scope =
        scope(
            "agg_none",
            "55555555-5555-5555-5555-555555555504",
            "66666666-6666-6666-6666-666666666604");
    bootstrapSchema(scope.database());
    try (ITableSessionPool pool = newPool(scope.database())) {
      IoTDBTableConfig config = config(3);
      IoTDBTableTimeseriesWriter writer = new IoTDBTableTimeseriesWriter(pool, config);
      IoTDBTableTimeseriesDao dao = new IoTDBTableTimeseriesDao(pool, writer, config);
      try {
        saveAll(
            dao,
            scope,
            List.of(
                entry(1000L, "n", DataType.LONG, 7L),
                entry(1500L, "n", DataType.LONG, 8L),
                entry(2100L, "n", DataType.LONG, 9L)));

        ReadTsKvQuery raw = new BaseReadTsKvQuery("n", 1000L, 3000L, 10, "ASC");
        ReadTsKvQueryResult rawResult =
            dao.findAllAsync(scope.tenantId(), scope.entityId(), List.of(raw))
                .get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(0);

        // Aggregation.NONE returns every raw row (not bucketed) at its own timestamp.
        assertEquals(3, rawResult.getData().size());
        assertEquals(1000L, rawResult.getData().get(0).getTs());
        assertEquals(1500L, rawResult.getData().get(1).getTs());
        assertEquals(2100L, rawResult.getData().get(2).getTs());
        assertEquals(9L, rawResult.getData().get(2).getValue());
      } finally {
        dao.destroy();
        writer.destroy();
      }
    }
  }

  private ReadTsKvQueryResult aggregate(
      IoTDBTableTimeseriesDao dao, TestScope scope, String key, Aggregation aggregation)
      throws Exception {
    ReadTsKvQuery query =
        new BaseReadTsKvQuery(key, 1000L, 3000L, INTERVAL, 100, aggregation, "ASC");
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

  private void assertStringBuckets(
      ReadTsKvQueryResult result, long[] expectedTs, String[] expectedValues) {
    List<TsKvEntry> data = result.getData();
    assertEquals(expectedTs.length, data.size(), "bucket count");
    for (int i = 0; i < expectedTs.length; i++) {
      TsKvEntry entry = data.get(i);
      assertEquals(expectedTs[i], entry.getTs(), "bucket ts at index " + i);
      assertEquals(DataType.STRING, entry.getDataType(), "data type at index " + i);
      assertEquals(
          Optional.of(expectedValues[i]), entry.getStrValue(), "string value at index " + i);
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

  /**
   * Like {@link #assertNumericBuckets} but keeps the expected LONG values in a {@code long[]} so a
   * value > 2^53 cannot be silently rounded by the test itself (the {@code double[]}-based helper
   * would lose the low bit of {@code 9007199254740993L}). For a LONG bucket the value is compared
   * EXACTLY via {@code getLongValue()}; for a DOUBLE bucket {@code expectedDoubleValues[i]} is
   * compared with a 1e-9 tolerance. This is the assertion that proves the precision contract: under
   * a COALESCE->DOUBLE round-trip (MIN/MAX) and DOUBLE SUM accumulator the long results would come
   * back as {@code ...992} and fail here.
   */
  private void assertExactNumericBuckets(
      ReadTsKvQueryResult result,
      long[] expectedTs,
      DataType[] expectedTypes,
      long[] expectedLongValues,
      double[] expectedDoubleValues,
      long expectedLastEntryTs) {
    List<TsKvEntry> data = result.getData();
    assertEquals(expectedTs.length, data.size(), "bucket count");
    for (int i = 0; i < expectedTs.length; i++) {
      TsKvEntry entry = data.get(i);
      assertEquals(expectedTs[i], entry.getTs(), "bucket ts at index " + i);
      assertEquals(expectedTypes[i], entry.getDataType(), "data type at index " + i);
      if (expectedTypes[i] == DataType.LONG) {
        assertTrue(entry.getLongValue().isPresent(), "long value present at index " + i);
        assertEquals(
            expectedLongValues[i],
            entry.getLongValue().get().longValue(),
            "exact long value at index " + i);
      } else {
        assertTrue(entry.getDoubleValue().isPresent(), "double value present at index " + i);
        assertEquals(
            expectedDoubleValues[i],
            entry.getDoubleValue().get(),
            1e-9,
            "double value at index " + i);
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
        IoTDBTableTimeseriesAggregationIT.class
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
