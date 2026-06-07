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

# IoTDB ThingsBoard Table — Benchmarks

This directory documents the performance test cases for the IoTDB Table Mode
ThingsBoard storage backend. It currently covers **TC-1 (ingestion
throughput)**, design document section 7.

## Two profiles

Design document section 7.1 defines two profiles for each test case:

- **Smoke profile** — small, fast, reproducible on a laptop in well under ten
  minutes; single tenant; not wired into required CI yet, though it is
  CI-eligible wherever Docker is available. Its purpose is to exercise the real
  write path end to end and guard against gross regressions. **This is what is
  implemented today.**
- **Full profile** — a dedicated host, contributor-run, multi-backend
  comparison (Cassandra / PostgreSQL / TimescaleDB). It is *not* in CI and is
  **later-scope**; its report lives in [`report.md`](report.md) (placeholder).

## TC-1 — Ingestion throughput

Design intent (section 7): 1,000 devices writing simultaneously via 50
concurrent threads in 500-entry batches; measure records/sec and error rate.
The week-6 calendar target is **> 10,000 writes/sec**.

That **> 10K writes/sec figure is the full-profile headline on a dedicated
host.** A cold single-node container on a laptop or CI runner will not reach it,
so the smoke profile does not assert it.

### What the smoke benchmark does

`IoTDBTableIngestionBenchmarkIT` drives the **real** save path — the same code
ThingsBoard uses in production:

```
dao.save(tenant, entity, tsKvEntry, ttl)
  -> writer.enqueue(...)           bounded ArrayBlockingQueue (capacity 50,000)
  -> single flush worker           batches up to 500 rows, maxLingerMs 20
  -> Tablet insert                 multi-row table-session insert
  -> real IoTDB 2.0.8              apache/iotdb:2.0.8-standalone Testcontainer
```

It runs `SAVER_THREADS = 50` concurrent threads, each writing
`ROWS_PER_THREAD = 600` rows (30,000 rows total) with a distinct
`(entity, key, timestamp)` per write so nothing is deduplicated away. The
production save defaults are used unchanged (batchSize 500, queueCapacity
50,000, maxLingerMs 20, flushThreads 1, sessionPoolSize 8); only the retry
backoff is shortened so a transient cold-start blip does not stretch the
measured window. The total row count is kept below the queue capacity so the
run is free of back-pressure rejects without changing the real defaults.

### What it measures and asserts

Measured and logged:

- **records/sec** — `totalRows / wall-clock seconds`, timed from the first
  `save()` to all save futures completing.
- **error rate** — `failedFutures / totalRows`.
- **writer stats** — `dao.stats()`: `enqueued`, `flushed`, `flushFailures`,
  `retries`, `rejectsFull`, `rejectsShutdown`, `queueDepth`.
- **persisted-sample count** — a handful of rows are read back from IoTDB to
  prove real ingestion, not just future completion.

Asserted:

- error rate `== 0` and zero failed save futures;
- `flushFailures == 0`, `rejectsFull == 0`, `rejectsShutdown == 0`;
- `flushed == totalRows` (every distinct row reached IoTDB);
- the sampled rows are readable back from IoTDB;
- throughput `>=` a **conservative smoke floor of 1,000 rows/sec**.

#### Why the floor is 1,000 rows/sec, not 10,000

The smoke floor only guards against gross regressions and proves correctness on
a cold, shared, single-node container. It is intentionally an order of magnitude
below the full-profile headline so the test is not flaky on laptops or CI. Raise
it only alongside a measured full-profile report — never to chase the headline
number on CI.

### How to run it locally

The benchmark is named `*IT.java` and tagged `@Tag("benchmark")`. The unit
`mvn test` run never executes it, and the default `verify`/CI run also excludes
it (`failsafe.excluded.groups=benchmark`) so it never adds its multi-minute,
Docker-backed cost to normal builds. Run it explicitly (Docker required) by
clearing the exclusion and selecting the tag group:

```bash
mvn -o -Dmaven.repo.local=<repo> verify -Dfailsafe.excluded.groups= -Dgroups=benchmark
```

Select by tag group, **not** `-Dtest=IoTDBTableIngestionBenchmarkIT`: a global
`-Dtest=` overrides the include/exclude filters of the surefire unit-test
execution, which would pull the Docker IT into the unit `test` phase.
`-Dgroups=benchmark`
applies the tag filter on top of the file patterns, and
`-Dfailsafe.excluded.groups=` clears the default benchmark exclusion so the
failsafe execution actually runs it.

A plain `mvn -o -Dmaven.repo.local=<repo> verify` runs the unit tests and the
functional `*IT.java` integration tests but skips this benchmark.

The benchmark is tagged `@Tag("benchmark")` and `@Tag("integration")`. The
measured records/sec and the full writer-stats report are emitted to the test
log at INFO and to stdout.

If Docker is unavailable the test is skipped
(`@Testcontainers(disabledWithoutDocker = true)`); it never fails the build for
lack of Docker.

### Out-of-band smoke stack

The benchmark IT manages its own throwaway Testcontainer, so no external stack
is required. For a manual smoke run against a standalone node, a clean-volume
IoTDB service is provided:

```bash
IOTDB_USERNAME=<iotdb-user> IOTDB_PASSWORD=<iotdb-password> \
  docker compose -f docker-compose.bench.yml up -d

# reset to an empty store between runs
docker compose -f docker-compose.bench.yml down -v
```

## Full-profile report

The full-profile multi-backend report is deferred; see
[`report.md`](report.md).
