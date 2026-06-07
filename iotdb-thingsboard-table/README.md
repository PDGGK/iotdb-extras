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

`iotdb-thingsboard-table` is the Week 1 scaffold for GSOC-304: enhancing the
ThingsBoard integration with Apache IoTDB 2.x Table Mode. It is a standalone
Maven module skeleton for the future DAO implementation and is not registered
in the `iotdb-extras` parent reactor yet. The canonical design document is
published on Drive:
https://drive.google.com/file/d/1jXMCwF_HVvCR5lHDIT_1pv1DiIZt8j5O/view?usp=sharing

## Build

From the module directory:

```bash
cd iotdb-thingsboard-table
mvn compile -DskipTests
```

If the parent POM has to be installed into a local Maven repository first (one-time, when the module is not yet wired into the `iotdb-extras` reactor `<modules>`):

```bash
# from the iotdb-extras repository root
mvn -N install -DskipTests
```

## Test

Run the Java test scaffold from the module directory:

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

Wk 1 scaffold — method bodies pending dev list feedback.
