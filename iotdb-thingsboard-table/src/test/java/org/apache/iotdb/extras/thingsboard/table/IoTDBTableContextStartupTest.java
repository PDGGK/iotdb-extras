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

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IoTDBTableContextStartupTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TableContextTestConfiguration.class);

  @Test
  void noActivation_contextStartsWithoutIoTDBBeans() {
    contextRunner.run(
        context -> {
          assertFalse(context.containsBean("tableSessionPool"));
          assertFalse(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
          assertFalse(context.containsBeanDefinition("ioTDBTableLatestDao"));
          assertFalse(context.containsBeanDefinition("ioTDBTableLabelDao"));
        });
  }

  @Test
  void tsTypeActivation_createsPoolAndTimeseries() {
    contextRunner
        .withPropertyValues(
            "database.ts.type=iotdb-table",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000")
        .run(
            context -> {
              assertTrue(context.containsBean("tableSessionPool"));
              assertTrue(context.getBean(ITableSessionPool.class) != null);
              assertTrue(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
              assertTrue(context.getBean(IoTDBTableTimeseriesDao.class) != null);
              assertFalse(context.containsBeanDefinition("ioTDBTableLatestDao"));
              assertFalse(context.containsBeanDefinition("ioTDBTableLabelDao"));
            });
  }

  @Test
  void tsLatestTypeActivation_createsPoolAndLatest() {
    contextRunner
        .withPropertyValues(
            "database.ts_latest.type=iotdb-table",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000")
        .run(
            context -> {
              assertTrue(context.containsBean("tableSessionPool"));
              assertTrue(context.containsBeanDefinition("ioTDBTableLatestDao"));
              assertFalse(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
              assertFalse(context.containsBeanDefinition("ioTDBTableLabelDao"));
            });
  }

  @Test
  void labelsEnabledActivation_createsPoolAndLabel() {
    contextRunner
        .withPropertyValues(
            "iotdb.labels.enabled=true",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000")
        .run(
            context -> {
              assertTrue(context.containsBean("tableSessionPool"));
              assertTrue(context.containsBeanDefinition("ioTDBTableLabelDao"));
              assertFalse(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
              assertFalse(context.containsBeanDefinition("ioTDBTableLatestDao"));
            });
  }

  @Test
  void uppercaseSelector_stillActivatesPoolAndDao() {
    contextRunner
        .withPropertyValues(
            "database.ts.type=IOTDB-TABLE",
            "iotdb.host=localhost",
            "iotdb.port=6667",
            "iotdb.username=root",
            "iotdb.password=root",
            "iotdb.session-pool-size=8",
            "iotdb.connection-timeout-ms=5000")
        .run(
            context -> {
              assertTrue(context.containsBean("tableSessionPool"));
              assertTrue(context.containsBeanDefinition("ioTDBTableTimeseriesDao"));
            });
  }

  @Configuration
  @Import(IoTDBTableConfiguration.class)
  @ComponentScan(basePackageClasses = IoTDBTableBaseDao.class)
  static class TableContextTestConfiguration {}
}
