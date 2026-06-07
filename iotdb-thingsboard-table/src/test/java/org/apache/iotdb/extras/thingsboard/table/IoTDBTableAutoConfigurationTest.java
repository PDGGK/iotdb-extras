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

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.DeleteTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQueryResult;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Auto-configuration tests that drive the module through Spring Boot's real auto-configuration
 * discovery path ({@link AutoConfigurations#of}, the mechanism a real ThingsBoard Spring Boot app
 * uses via {@code META-INF/spring/...AutoConfiguration.imports}), rather than a plain
 * {@code @Import}. This proves the META-INF auto-config registration actually activates the module
 * the way a deployed application would, and that the DAO backs off when the host already provides a
 * {@link TimeseriesDao}.
 */
class IoTDBTableAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(IoTDBTableConfiguration.class));

  @Test
  void autoConfigDiscovery_withSelector_createsPoolAndDao() {
    contextRunner
        .withPropertyValues(
            "database.ts.type=iotdb-table",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000",
            // Offline test with no real IoTDB: skip the startup bootstrap so afterPropertiesSet()
            // never opens a session.
            "iotdb.schema.bootstrap=false")
        .run(
            context -> {
              assertTrue(context.containsBean("tableSessionPool"));
              assertTrue(context.getBean(ITableSessionPool.class) != null);
              assertTrue(context.containsBean("ioTDBTableTimeseriesDao"));
              assertTrue(context.getBean(IoTDBTableTimeseriesDao.class) != null);
            });
  }

  @Test
  void autoConfigDiscovery_withoutSelector_createsNoBeans() {
    contextRunner.run(
        context -> {
          assertFalse(context.containsBean("tableSessionPool"));
          assertFalse(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
          assertFalse(context.containsBeanDefinition("schemaBootstrap"));
        });
  }

  // Back-off guard: when the host already provides a TimeseriesDao bean (any implementation), the
  // module's @ConditionalOnMissingBean(TimeseriesDao.class) DAO registration must back off so the
  // two SPI beans never collide as duplicate beans.
  @Test
  void hostProvidedTimeseriesDao_makesModuleDaoBackOff() {
    contextRunner
        .withUserConfiguration(HostTimeseriesDaoConfiguration.class)
        .withPropertyValues(
            "database.ts.type=iotdb-table",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000",
            "iotdb.schema.bootstrap=false")
        .run(
            context -> {
              // The pool still comes up (it activates on the selector), but the module's DAO bean
              // must not be registered because a TimeseriesDao is already present.
              assertTrue(context.containsBean("tableSessionPool"));
              assertFalse(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
              // The only TimeseriesDao in the context is the host-provided one, and it is not an
              // instance of the module DAO.
              TimeseriesDao dao = context.getBean(TimeseriesDao.class);
              assertSame(HostTimeseriesDaoConfiguration.HOST_DAO, dao);
              assertFalse(dao instanceof IoTDBTableTimeseriesDao);
            });
  }

  @Configuration
  static class HostTimeseriesDaoConfiguration {
    static final TimeseriesDao HOST_DAO = new NoopTimeseriesDao();

    @Bean
    TimeseriesDao hostTimeseriesDao() {
      return HOST_DAO;
    }
  }

  /** Minimal host-supplied {@link TimeseriesDao} used only to trigger the back-off conditional. */
  private static final class NoopTimeseriesDao implements TimeseriesDao {
    @Override
    public ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(
        TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Integer> save(
        TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Integer> savePartition(
        TenantId tenantId, EntityId entityId, long tsKvEntryTs, String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> remove(
        TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup(long systemTtl) {
      throw new UnsupportedOperationException();
    }
  }
}
