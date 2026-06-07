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

The module is wired into the `iotdb-extras` parent reactor `<modules>`, so the
root build already compiles and tests it. This file is a developer reference of
the local checks for the `iotdb-thingsboard-table` module; it is not itself a
GitHub Actions workflow.

## Candidate Checks

- Compile from the standalone module directory:
  `mvn compile -DskipTests`
- Run unit tests:
  `mvn test`
- Validate the local stack file:
  `docker compose -f docker-compose.test.yml config`
- Run an integration profile only when Docker is available and required
  environment values are set:
  `TB_POSTGRES_USER=<postgres-user> TB_POSTGRES_PASSWORD=<postgres-password> IOTDB_USERNAME=<iotdb-user> IOTDB_PASSWORD=<iotdb-password>`

## Notes

- Keep this file inside the module. Do not copy it to `.github/workflows`.
- Do not store passwords, tokens, or local hostnames in CI configuration.
- Keep the Docker image tags aligned with the preflight result for this module.
