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

# CI Notes

This file is a future CI template for the `iotdb-thingsboard-table` module. It
is not a GitHub Actions workflow.

## Candidate Checks

- Compile from the standalone module directory:
  `mvn compile -DskipTests`
- Run unit tests:
  `mvn test`
- Validate the local stack files:
  `docker compose -f docker-compose.test.yml config` and
  `docker compose -f docker-compose.bench.yml config`
- Run the TC-1 ingestion-throughput smoke benchmark (design doc section 7) only
  when Docker is available; it runs in the `integration-test` phase, not the
  unit run. Select it by tag group (a global `-Dtest=` would override both
  surefire executions' filters and leak the Docker IT into the unit phase):
  `mvn integration-test -Dgroups=benchmark`
- Run an integration profile only when Docker is available and required
  environment values are set:

  ```bash
  TB_POSTGRES_USER=<postgres-user> TB_POSTGRES_PASSWORD=<postgres-password> \
    IOTDB_USERNAME=<iotdb-user> IOTDB_PASSWORD=<iotdb-password>
  ```

## Notes

- Keep this file inside the module. Do not copy it to `.github/workflows`.
- Do not store passwords, tokens, or local hostnames in CI configuration.
- Keep the Docker image tags aligned with the preflight result for this module
  deliverable.
- Add reactor and GitHub Actions wiring only after the relevant scope gate.
