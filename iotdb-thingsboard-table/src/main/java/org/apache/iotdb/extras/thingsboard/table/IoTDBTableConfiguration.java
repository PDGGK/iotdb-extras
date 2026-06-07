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
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Spring Boot auto-configuration entry point for the IoTDB Table Mode backend.
 *
 * <p>This class is registered via {@code
 * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports} (Spring Boot
 * 3.x mechanism) and, as a belt-and-suspenders fallback, via {@code META-INF/spring.factories}, so
 * the module activates in a real ThingsBoard deployment without the host application having to
 * component-scan {@code org.apache.iotdb.extras}.
 *
 * <p>The {@code @Bean} methods below explicitly register the session pool, the timeseries writer,
 * the schema bootstrap, and the {@code @Repository} {@link IoTDBTableTimeseriesDao}. Explicit bean
 * methods are used in preference to {@code @ComponentScan}, which Spring deliberately filters out
 * of auto-configuration classes (it would otherwise re-scan the host application's packages). Each
 * bean keeps its own activation conditional so the context stays inert unless {@code
 * database.ts.type=iotdb-table} (or a sibling selector) is set.
 */
@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(IoTDBTableConfig.class)
public class IoTDBTableConfiguration {

  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean(ITableSessionPool.class)
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

  /**
   * Registers the historical-telemetry DAO. The DAO type is annotated {@code @Repository} for
   * documentation and for component-scan-based deployments, but auto-configuration registers it
   * explicitly here (auto-config classes do not honor {@code @ComponentScan}). The bean name {@code
   * ioTDBTableTimeseriesDao} matches the default component-scan name, and
   * {@code @ConditionalOnMissingBean} makes this registration back off if a host has already
   * component-scanned the DAO, so the two paths never collide with a duplicate-bean error. The
   * remaining conditionals mirror the ones on {@link IoTDBTableTimeseriesDao} so activation stays
   * gated on the pool bean and {@code database.ts.type=iotdb-table}.
   */
  @Bean
  @ConditionalOnBean(ITableSessionPool.class)
  @ConditionalOnMissingBean(IoTDBTableTimeseriesDao.class)
  @ConditionalOnProperty(name = "database.ts.type", havingValue = "iotdb-table")
  public IoTDBTableTimeseriesDao ioTDBTableTimeseriesDao(
      ITableSessionPool tableSessionPool,
      IoTDBTableTimeseriesWriter timeseriesWriter,
      IoTDBTableConfig config) {
    return new IoTDBTableTimeseriesDao(tableSessionPool, timeseriesWriter, config);
  }

  /**
   * Idempotent startup schema bootstrap. Only registered when the IoTDB Table Mode backend is
   * actually selected (same activation guard as the pool/DAO), the session pool bean is present,
   * and {@code iotdb.schema.bootstrap} is not disabled (defaults to {@code true}), so a fresh IoTDB
   * gets the {@code telemetry}/{@code entity_attributes} tables created before the first write. The
   * backend guard prevents this from running ThingsBoard schema DDL when an unrelated host happens
   * to define an {@link ITableSessionPool} without choosing this backend. Operators managing the
   * schema out-of-band can set {@code iotdb.schema.bootstrap=false} to skip it.
   */
  @Bean
  @ConditionalOnBean(ITableSessionPool.class)
  @ConditionalOnExpression(
      "'${database.ts.type:}'.equalsIgnoreCase('iotdb-table') "
          + "or '${database.ts_latest.type:}'.equalsIgnoreCase('iotdb-table') "
          + "or '${iotdb.labels.enabled:false}'.equalsIgnoreCase('true')")
  @ConditionalOnProperty(
      name = "iotdb.schema.bootstrap",
      havingValue = "true",
      matchIfMissing = true)
  public IoTDBTableSchemaBootstrap schemaBootstrap(
      ITableSessionPool tableSessionPool, IoTDBTableConfig config) {
    return new IoTDBTableSchemaBootstrap(tableSessionPool, config);
  }
}
