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

`iotdb-thingsboard-table` is a ThingsBoard historical-telemetry DAO backend
built on Apache IoTDB 2.0.8 Table Mode (GSOC-304: enhancing the ThingsBoard
integration with IoTDB 2.x Table Mode). It lets a ThingsBoard deployment store
and serve time-series telemetry through IoTDB's table-session API instead of the
default Cassandra/SQL backends. The module targets ThingsBoard v4.3.1.1. It is
wired into the `iotdb-extras` parent reactor `<modules>`, so it builds and tests
as part of the root project. The canonical design document is published on Drive:
https://drive.google.com/file/d/1jXMCwF_HVvCR5lHDIT_1pv1DiIZt8j5O/view?usp=sharing

## ThingsBoard SPI surface (Strategy F)

The ThingsBoard DAO SPI and value types (`org.thingsboard.*`) are not published
to Maven Central, so they cannot be a normal compile dependency. Strategy F
treats them as a **compile-only source surface** under `src/provided/java`: just
enough of the ThingsBoard interfaces and value objects to compile against. The
maven-jar-plugin excludes `org/thingsboard/**` from the built jar, so these
compile-only types never ship and never shadow the real ThingsBoard classes. At
runtime the actual ThingsBoard classpath supplies them. This keeps the module
buildable in isolation while binding to the genuine ThingsBoard types on a real
deployment.

## Scope (staged PR series)

This is **PR-1 of a staged series** (design doc section 6.3). It delivers the
`IoTDBTableBaseDao` (session-pool lifecycle, schema/table bootstrap) and the
`IoTDBTableTimeseriesDao` write path (`save`), raw read, and delete. Aggregation,
latest telemetry, and attribute/label DAOs are inert scaffolds in this PR and are
implemented in later PRs.

## Build

The module builds from the repository root as part of the reactor:

```bash
# from the iotdb-extras repository root
mvn -pl iotdb-thingsboard-table -am clean test
```

It can also be built standalone from the module directory:

```bash
cd iotdb-thingsboard-table
mvn compile -DskipTests
```

## Test

Run the Java test scaffold from the module directory:

```bash
mvn test
```

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

## Status

PR-1 of the staged series (design doc section 6.3): `IoTDBTableBaseDao` plus the
`IoTDBTableTimeseriesDao` write, raw-read, and delete paths are implemented.
Aggregation, latest telemetry, and attribute/label DAOs are inert scaffolds and
land in later PRs.
