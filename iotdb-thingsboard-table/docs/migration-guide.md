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

# Migrating ThingsBoard telemetry to IoTDB Table Mode

This is a **draft (WK12)** migration outline for moving existing ThingsBoard
telemetry from a relational/Cassandra backend into the IoTDB Table Mode
`telemetry` table. It describes the data-shape mapping and the safe cut-over
order; the detailed tooling/runbook tracks the GSOC-304 design document. See the
module [`README.md`](../README.md) and [`docs/user-guide.md`](user-guide.md) for
configuration and limitations.

## Scope

- **Telemetry (`ts_kv`) → `telemetry`**: in scope.
- **Latest (`ts_kv_latest`)**: NOT migrated as a separate store — this backend
  derives the latest value from `telemetry` (no shadow latest table), so once
  history is migrated the latest follows automatically.
- **Attributes (`attribute_kv` → `entity_attributes`)**: applies **only if** the
  `AttributesDao` activation Path (1) is accepted upstream (open question Q6,
  ThingsBoard Discussion #15296). Under the Path (3) fallback, attributes remain
  in the entity database and are out of migration scope.

## Data-shape mapping (`ts_kv` → `telemetry`)

ThingsBoard's relational `ts_kv` stores one row per `(entity_id, key, ts)` with
the value in one typed column (`bool_v` / `long_v` / `dbl_v` / `str_v` /
`json_v`) and `key` dictionary-encoded. The IoTDB `telemetry` table keys each
row by the TAG tuple `(tenant_id, entity_type, entity_id, key)` plus the built-in
`time`, with the value in the matching FIELD column:

| ThingsBoard `ts_kv` | IoTDB `telemetry` |
|---------------------|-------------------|
| `entity_id` (+ entity type) | `entity_id`, `entity_type` TAGs |
| *(tenant)* | `tenant_id` TAG |
| `key` (dictionary id → string) | `key` TAG (resolved to the string key) |
| `ts` | `time` |
| `bool_v` | `bool_v` FIELD |
| `long_v` | `long_v` FIELD |
| `dbl_v` | `double_v` FIELD |
| `str_v` | `str_v` FIELD |
| `json_v` | `json_v` FIELD |

Exactly one value column is non-null per row, preserving the source shape.

## Cut-over outline

1. **Stand up IoTDB** 2.0.8 (Table Mode) and create the schema
   (`schema-iotdb-table.sql`) in the target `iotdb.database`. Set a concrete
   table-level TTL on `telemetry` if you want retention (see README
   "Retention / TTL").
2. **Backfill** historical `ts_kv` into `telemetry`: read from the source
   backend (resolving the `key` dictionary to string keys and supplying the
   `tenant_id` / `entity_type` for each `entity_id`) and write via batched
   `Tablet` inserts into `telemetry`. The same value-column mapping above
   applies. This step is operator/ETL-driven; size batches for throughput (the
   module's own write path uses 500-row Tablet batches — see the TC-1 benchmark).
3. **Verify** a sample: spot-check historical reads and latest values for a set
   of `(entity, key)` pairs against the source.
4. **Switch** ThingsBoard to the IoTDB backend
   (`database.ts.type=iotdb-table`, `database.ts_latest.type=iotdb-table`) and
   restart. New writes flow into `telemetry`; the derived latest is immediately
   correct because it reads from the same table.

## Caveats

- **Idempotency**: re-running the backfill for the same `(tags, time)` overwrites
  the typed column in place (IoTDB same-timestamp overwrite), so re-runs converge
  rather than duplicate — but a value whose *type* changed at the same timestamp
  is subject to the same-timestamp limitation documented in the README.
- **Latest**: do not attempt to migrate `ts_kv_latest` separately; it is derived.
- **Attributes**: gated on Q6 (see Scope).
- **Ordering / dual-write**: for zero-downtime cut-over, a dual-write or
  read-old/write-new bridge during backfill is an operator concern beyond this
  draft; the detailed strategy tracks the design document.

> This draft will be expanded into a full runbook (concrete ETL tooling,
> dictionary resolution, batching, validation queries, and the dual-write
> strategy) as WK12 progresses and once the Q6 attribute-activation path is
> resolved.
