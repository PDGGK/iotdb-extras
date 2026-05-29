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

/**
 * Attribute DAO skeleton for the IoTDB Table Mode backend.
 *
 * <p>Spring activation: TBD by Q6; no conditional property is declared in this scaffold because
 * ThingsBoard does not yet expose an AttributesDao selector.
 *
 * <p>Strategy F keeps this class free of ThingsBoard imports and interface clauses until the DAO
 * dependency path is decided.
 *
 * @see "GSOC-304 design doc section 6.0"
 * @since GSOC-304 Wk 1 scaffold
 */
@Slf4j
public class IoTDBTableAttributesDao extends IoTDBTableBaseDao {
  // Not annotated @Repository yet — Q6 activation property is TBD; Wk 5 will add the appropriate
  // @ConditionalOnProperty once Q6 resolves (see TB Discussion #15296).
  public IoTDBTableAttributesDao(ITableSessionPool tableSessionPool) {
    super(tableSessionPool);
  }

  // TODO(Q6): activation property waits for ThingsBoard maintainer decision.
  // TODO(Strategy F): add AttributesDao binding after dependency resolution is decided.
  // TODO(GSOC-304 Wk 5): add attribute method bodies after Q6 is resolved.
}
