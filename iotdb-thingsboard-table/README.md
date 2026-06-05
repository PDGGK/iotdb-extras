<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# IoTDB ThingsBoard Table

## Overview

`iotdb-thingsboard-table` is the GSOC-304 standalone Maven module for enhancing
the ThingsBoard integration with Apache IoTDB 2.x Table Mode.
`IoTDBTableTimeseriesDao` implements the `TimeseriesDao` SPI: `save()` is the
historical telemetry write path (bounded batch queue, single flush worker,
Tablet writes, connection retry, back-pressure rejection, graceful shutdown
drain), `findAllAsync()` is the non-aggregation historical read (half-open time
window, order/limit), and `remove()` is the half-open range delete;
`savePartition()` is a no-op and aggregation reads are later scope.
`IoTDBTableLatestDao` implements the `TimeseriesLatestDao` SPI as a single-table
derived latest (no shadow table, backed by IoTDB Table Mode's native last
cache): `findLatest`/`findLatestOpt` via `ORDER BY time DESC LIMIT 1`,
`findAllLatest` via `LAST_BY(..., time) GROUP BY key`, key discovery, a no-op
`saveLatest`, and a window-aware `removeLatest`. The module is not registered in
the `iotdb-extras` parent reactor yet. The canonical design document is
published on Drive:
https://drive.google.com/file/d/1jXMCwF_HVvCR5lHDIT_1pv1DiIZt8j5O/view?usp=sharing

The ThingsBoard common-data types consumed by the DAO are currently provided at
compile time through the `src/provided/java` fallback source root and excluded
from the built jar. Replace that fallback with a provided-scope
`org.thingsboard.common:data` dependency once the real artifact is resolvable in
the build environment.

## Build

From the module directory:

```bash
cd iotdb-thingsboard-table
mvn compile -DskipTests
```

If the parent POM has to be installed into a local Maven repository first
(one-time, when the module is not yet wired into the `iotdb-extras` reactor
`<modules>`):

```bash
# from the iotdb-extras repository root
mvn -N install -DskipTests
```

## Test

Run the unit tests from the module directory:

```bash
mvn test
```

`mvn test` compiles the Docker-backed `*IT.java` integration tests (including
the TC-1 ingestion-throughput benchmark, `IoTDBTableIngestionBenchmarkIT`) but
leaves them to the `integration-test` phase; the unit run never executes them.

Start the local integration stack with explicit environment values:

```bash
TB_POSTGRES_USER=<postgres-user> TB_POSTGRES_PASSWORD=<postgres-password> \
  IOTDB_USERNAME=<iotdb-user> IOTDB_PASSWORD=<iotdb-password> \
  docker compose -f docker-compose.test.yml up -d
```

Stop and remove the local stack:

```bash
docker compose -f docker-compose.test.yml down -v
```

## Benchmarks

The TC-1 ingestion-throughput benchmark (design doc section 7, smoke profile)
runs the real `save()` path against a throwaway IoTDB Testcontainer and reports
records/sec, error rate, and writer stats. See
[`docs/benchmarks/README.md`](docs/benchmarks/README.md) for how to run it and
for the smoke floor versus the full-profile `> 10K writes/sec` headline target.

## Retention / TTL

Physical retention is a **table-level** IoTDB property, set by the operator on
the schema — not a per-data-point setting. IoTDB Table Mode expresses TTL as a
retention window in **milliseconds**, given as a bare (unquoted) long literal,
or the keyword `INF` (never expire) / `DEFAULT` (inherit the database default,
which is `INF` on a fresh node). Quoted numbers (`'604800000'`) and duration
forms (`'7d'`) are rejected by IoTDB 2.0.8.

The shipped `schema-iotdb-table.sql` declares the `telemetry` table with
`WITH (TTL=DEFAULT)`. To enable a concrete retention, an operator either edits
the schema before bootstrap, e.g. 7 days:

```sql
CREATE TABLE telemetry (...) WITH (TTL=604800000);
```

or changes it at runtime on the live table:

```sql
ALTER TABLE telemetry SET PROPERTIES TTL=604800000;   -- 7 days, in ms
ALTER TABLE telemetry SET PROPERTIES TTL=DEFAULT;     -- back to the db default
```

The effective TTL can be read back from `information_schema.tables` (the
`ttl(ms)` column) or via `SHOW TABLES` (the `TTL(ms)` column). The
`IoTDBTableTtlIT` integration test verifies all of this against real IoTDB
2.0.8. It validates the TTL property mechanism only; it does not assert physical
row eviction, because IoTDB TTL eviction is asynchronous and compaction-driven
and so is not deterministic within a test.

- **Phase-1 limitation: the per-save `ttl` argument is not honored per-row.**
  The `TimeseriesDao.save(..., long ttl)` SPI carries a per-data-point TTL, but
  IoTDB Table Mode TTL is table-wide; the two cannot be faithfully reconciled,
  because physical retention can only be expressed at the table level. The
  module therefore uses the per-save `ttl` only for ThingsBoard's storage
  data-point accounting (`iotdb.defaultTtlMs` participates in that accounting
  too) and never as a physical-retention directive. Operators who need physical
  retention set it on the table as shown above. A config-driven,
  module-applied table TTL (e.g. the DAO issuing
  `ALTER TABLE telemetry SET PROPERTIES TTL=<iotdb.defaultTtlMs>` at startup) is
  a possible future enhancement, deliberately deferred pending mentor input on
  whether the module or the operator's schema should own retention DDL.

## Status

The `TimeseriesDao` write/read/remove paths and the `TimeseriesLatestDao`
latest-value paths are implemented and covered by unit tests plus Docker-backed
integration tests (schema bootstrap, Table Mode writes, historical reads,
deletes, and derived latest). Aggregation reads, the latest shadow table, and
the full delete-then-insert same-timestamp defense remain later-scope work.

- Phase-1 limitation, mentor-approved 2026-06-01: a cross-batch
  same-timestamp type-change can leave a stale row with two typed value columns.
  Raw point-reads fail fast on it. Aggregation does not fail, but is not fully
  consistent on such a malformed row: `AVG`/`MIN`/`MAX` apply `COALESCE` priority
  (use the double, ignore the long), while `SUM` and `COUNT` count both typed
  columns and can therefore double-count it. The full delete-then-insert defense
  (which prevents the malformed row entirely) is deferred to a later phase.
- Phase-1 limitation: the attribute `save` is delete-then-insert (two
  non-atomic IoTDB statements). If the INSERT fails or the process crashes in
  the window after the DELETE commits and before the INSERT commits, the prior
  committed attribute value is lost — not merely briefly unreadable. ThingsBoard
  itself uses an atomic UPSERT and has no such window. The full atomic-upsert /
  append-only defense is deferred to a later phase pending mentor input.
  Point-reads (`find` by single key) take the same per-identity lock and never
  observe the transient delete→insert gap; full-scope reads (`find` by key
  collection, `findAll` by scope) stay best-effort and may briefly miss a key
  whose `save` is in flight.
- Phase-1 limitation: `removeLatest` derives the latest from history (no shadow
  latest table). ThingsBoard submits the historical remove and `removeLatest` as
  separate futures, so if the historical delete commits first, `removeLatest`
  can read an empty/older latest and report `removed=false` even though the
  pre-delete latest was inside the delete window — a false negative that
  suppresses the latest-delete notification. The value/read semantics are
  unaffected; a robust fix needs a latest shadow/state (pre-delete snapshot) and
  is deferred pending mentor input on the single-table derived-latest design.
