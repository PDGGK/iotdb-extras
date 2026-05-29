/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.extras.thingsboard.table;

import org.apache.iotdb.isession.pool.ITableSessionPool;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

/**
 * Latest telemetry DAO skeleton for the IoTDB Table Mode backend.
 *
 * <p>Spring activation: database.ts_latest.type=iotdb-table.
 *
 * <p>Strategy F keeps this class free of ThingsBoard imports and interface clauses until the DAO
 * dependency path is decided.
 *
 * @see "GSOC-304 design doc section 6.0"
 * @since GSOC-304 Wk 1 scaffold
 */
@Slf4j
@Repository
@ConditionalOnBean(ITableSessionPool.class)
@ConditionalOnProperty(name = "database.ts_latest.type", havingValue = "iotdb-table")
public class IoTDBTableLatestDao extends IoTDBTableBaseDao {
  public IoTDBTableLatestDao(ITableSessionPool tableSessionPool) {
    super(tableSessionPool);
  }

  // TODO(Strategy F): add TimeseriesLatestDao binding after dependency resolution is decided.
  // TODO(GSOC-304 Wk 4): add latest telemetry method bodies.
}
