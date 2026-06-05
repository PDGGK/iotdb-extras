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

# IoTDB ThingsBoard Table — Operations & Configuration Guide

This guide covers configuring, activating, and operating the IoTDB Table Mode
storage backend for ThingsBoard. For the architecture overview, build/test
instructions, the TC-1 benchmark, retention/TTL, and the Phase-1 limitations,
see the module [`README.md`](../README.md). This is a draft (WK11) and tracks
the current Phase-1 behavior.

## 1. What it does

The module routes ThingsBoard telemetry and (optionally) attribute persistence
into Apache IoTDB 2.0.8 in relational **Table Mode**, implementing ThingsBoard's
`TimeseriesDao`, `TimeseriesLatestDao`, and `AttributesDao` SPIs. Telemetry is
written through a bounded batch queue and a single flush worker as multi-row
`Tablet` inserts; the latest value is derived from the single `telemetry` table
(IoTDB's native last cache accelerates `ORDER BY time DESC LIMIT 1` /
`LAST_BY`), so there is no separate latest/shadow table.

## 2. Prerequisites

- A running Apache **IoTDB 2.0.8** node with Table Mode (the relational SQL
  dialect) reachable from ThingsBoard.
- The IoTDB client RPC service must be reachable from the ThingsBoard host. IoTDB
  binds its RPC to `dn_rpc_address` (default `127.0.0.1`); set it to a reachable
  address/interface if ThingsBoard runs on a different host or in a container.
- The `telemetry` (and, if attributes are activated, `entity_attributes`) tables
  created from [`src/main/resources/schema-iotdb-table.sql`](../src/main/resources/schema-iotdb-table.sql)
  in the target database.

## 3. Activation

ThingsBoard selects its storage backends with `database.ts.type` and
`database.ts_latest.type`. Set both to `iotdb-table` to activate the IoTDB
time-series and latest DAOs:

```properties
database.ts.type=iotdb-table
database.ts_latest.type=iotdb-table
```

> **Attributes activation is Phase-1 / open.** ThingsBoard does not expose a
> `database.attributes.type` switch upstream. The `IoTDBTableAttributesDao`
> binds a Phase-1 selector `database.attributes.type=iotdb-table` pending the
> upstream decision (open question Q6, ThingsBoard Discussion #15296). Until that
> is resolved, attribute persistence may remain on the entity database. When the
> attribute DAO IS activated, `iotdb.attributes.cluster-mode` MUST be set (see
> below) or the DAO fails fast at startup.

## 4. Configuration reference (`iotdb.*`)

All properties are bound from the `iotdb` prefix. Defaults shown.

### Connection

| Property | Default | Notes |
|----------|---------|-------|
| `iotdb.host` | `127.0.0.1` | IoTDB node host. |
| `iotdb.port` | `6667` | IoTDB client RPC port (1–65535). |
| `iotdb.database` | `thingsboard` | Target Table Mode database. |
| `iotdb.username` | `root` | |
| `iotdb.password` | `root` | Set a real credential in production. |
| `iotdb.session-pool-size` | `8` | Sessions shared by the read + flush workers (1–1024). Keep `≥ ts.read.threads + ts.save.flush-threads`. |
| `iotdb.connection-timeout-ms` | `5000` | Min 100. |
| `iotdb.enable-compression` | `false` | RPC compression. |
| `iotdb.default-ttl-ms` | `-1` | **ThingsBoard data-point accounting only — does NOT set IoTDB physical retention** (IoTDB TTL is table-level; see README “Retention / TTL”). |

### Write path (`iotdb.ts.save.*`)

| Property | Default | Notes |
|----------|---------|-------|
| `batch-size` | `500` | Rows per `Tablet` insert. |
| `max-linger-ms` | `20` | Max time a partial batch waits to fill. |
| `queue-capacity` | `50000` | Bounded; over-capacity saves are rejected (back-pressure). |
| `flush-threads` | `1` | Fixed at 1 (single-writer ordering). |
| `shutdown-drain-timeout-ms` | `5000` | Graceful-drain window on shutdown. Keep `≥ max-linger-ms`. |
| `retry-max-attempts` | `3` | Per-batch retries on transient `IoTDBConnectionException`. |
| `retry-initial-backoff-ms` | `50` | |
| `retry-max-backoff-ms` | `1000` | |

### Read path (`iotdb.ts.read.*`)

| Property | Default | Notes |
|----------|---------|-------|
| `threads` | `4` | Bounded read executor. |
| `queue-capacity` | `10000` | Read-task queue bound. |

### Attributes (`iotdb.attributes.*`) — only when the attribute DAO is activated

| Property | Default | Notes |
|----------|---------|-------|
| `cluster-mode` | *(unset)* | Must be `sticky-routing` (writes for an identity pinned to one node) or `disabled` (single-node / acknowledged best-effort). Any other value (incl. unset) fails fast at startup. The per-identity write lock converges writes only within a single JVM; cross-node single-writer safety is the operator's responsibility. |

## 5. Deployment outline

1. Stand up IoTDB 2.0.8 with Table Mode reachable from ThingsBoard.
2. Create the schema (`schema-iotdb-table.sql`) in `iotdb.database`. Optionally
   set a concrete table-level TTL for `telemetry` (see README “Retention / TTL”).
3. Provide the `iotdb.*` config (at minimum host/port/database/credentials;
   tune the write/read pools for throughput).
4. Set `database.ts.type=iotdb-table` and `database.ts_latest.type=iotdb-table`
   (and, when Q6 resolves, the attributes selector + `iotdb.attributes.cluster-mode`).
5. Start ThingsBoard. Telemetry writes/reads now flow through IoTDB Table Mode.

## 6. Operational notes

- **Back-pressure**: if the save queue is saturated, saves are rejected with a
  queue-full error rather than blocking — size `ts.save.queue-capacity` for your
  ingest rate (see the TC-1 benchmark in [`docs/benchmarks/README.md`](benchmarks/README.md)).
- **Graceful shutdown**: on shutdown the flush worker drains accepted writes
  within `shutdown-drain-timeout-ms`; in-flight transient-error retries are
  allowed to finish within that window. A drain timeout fails the remaining
  pending writes (shutdown is at-least-once — tolerate a duplicate/uncertain
  final batch).
- **Retention**: physical retention is table-level (operator-set); the per-save
  `ttl` argument is not honored per-row. See README “Retention / TTL”.
- **Known Phase-1 limitations** (same-timestamp type change, attribute
  delete-then-insert durability, full-scope attribute read consistency, null
  attribute version / EDQS notifications): see the README “Status” section.
