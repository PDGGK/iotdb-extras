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
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@Configuration
@EnableConfigurationProperties(IoTDBTableConfig.class)
public class IoTDBTableConfiguration {

  @Bean(destroyMethod = "close")
  @ConditionalOnExpression(
      "'${database.ts.type:}'.equalsIgnoreCase('iotdb-table') "
          + "or '${database.ts_latest.type:}'.equalsIgnoreCase('iotdb-table') "
          + "or '${iotdb.labels.enabled:false}'.equalsIgnoreCase('true')")
  public ITableSessionPool tableSessionPool(IoTDBTableConfig config) {
    String nodeUrl = config.getHost() + ":" + config.getPort();
    ITableSessionPool pool =
        new TableSessionPoolBuilder()
            .nodeUrls(List.of(nodeUrl))
            .user(config.getUsername())
            .password(config.getPassword())
            .database(config.getDatabase())
            .maxSize(config.getSessionPoolSize())
            .connectionTimeoutInMs(config.getConnectionTimeoutMs())
            .enableIoTDBRpcCompression(config.isEnableCompression())
            .build();
    log.info(
        "IoTDB Table Mode session pool initialized: nodeUrl={}, database={}, poolSize={}, compression={}, storageAccountingDefaultTtlMs={}",
        nodeUrl,
        config.getDatabase(),
        config.getSessionPoolSize(),
        config.isEnableCompression(),
        config.getDefaultTtlMs());
    return pool;
  }

  @Bean
  @ConditionalOnProperty(name = "database.ts.type", havingValue = "iotdb-table")
  public IoTDBTableTimeseriesWriter timeseriesWriter(
      ITableSessionPool tableSessionPool, IoTDBTableConfig config) {
    return new IoTDBTableTimeseriesWriter(tableSessionPool, config);
  }
}
