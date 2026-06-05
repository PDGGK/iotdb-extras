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

# TC-1 Full-Profile Benchmark Report (placeholder)

> **Status: deferred / later-scope.**

This document will hold the **full-profile** TC-1 ingestion-throughput results
(design document section 7.1): 1,000 devices, 50 concurrent threads, 500-entry
batches, run on a dedicated host, with the **> 10,000 writes/sec** target and a
multi-backend comparison (Cassandra / PostgreSQL / TimescaleDB).

The full profile is not in CI and is run by a contributor on dedicated
hardware. The smoke-profile benchmark that runs locally today is described in
[`README.md`](README.md).

## To be filled in

- Hardware and IoTDB topology (single node vs. cluster).
- Dataset: device count, keys per device, batch size, total rows.
- Measured records/sec, error rate, and p50 / p99 batch flush latency.
- Per-backend comparison table.
- Tuning notes (session pool size, flush threads, queue capacity, linger).
