/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

CREATE DATABASE IF NOT EXISTS thingsboard;
USE thingsboard;

CREATE TABLE telemetry (
  tenant_id   STRING  TAG,    -- multi-tenant isolation
  entity_type STRING  TAG,    -- DEVICE, ASSET, etc.
  entity_id   STRING  TAG,    -- ThingsBoard entity UUID
  key         STRING  TAG,    -- telemetry key name
  bool_v      BOOLEAN FIELD,
  long_v      INT64   FIELD,  -- exactly one non-null per row,
  double_v    DOUBLE  FIELD,  -- mirroring AbstractTsKvEntity
  str_v       STRING  FIELD,
  json_v      TEXT    FIELD
)
-- Physical retention is a TABLE-LEVEL IoTDB property, expressed in MILLISECONDS.
-- DEFAULT inherits the database default (INF / never expire on a fresh node).
-- To enable retention, set a concrete value as a bare long literal in ms, e.g.
-- 7 days = 604800000:  WITH (TTL=604800000)
-- or change it at runtime:  ALTER TABLE telemetry SET PROPERTIES TTL=604800000;
-- Only an unquoted long literal, INF, or DEFAULT are accepted (no '7d' / quoted forms).
-- NOTE: this is table-wide; ThingsBoard's per-save ttl argument is not honored per-row
-- (see the README "Retention / TTL" section for the documented Phase-1 limitation).
WITH (TTL=DEFAULT);

CREATE TABLE entity_attributes (
  time            TIMESTAMP TIME,
  tenant_id       STRING TAG,
  entity_type     STRING TAG,
  entity_id       STRING TAG,
  attribute_scope STRING TAG,   -- CLIENT_SCOPE | SERVER_SCOPE | SHARED_SCOPE
  key             STRING TAG,
  bool_v BOOLEAN FIELD, long_v INT64 FIELD, double_v DOUBLE FIELD,
  str_v STRING FIELD, json_v TEXT FIELD
) WITH (TTL='INF');
