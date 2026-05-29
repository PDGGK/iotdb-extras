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

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IoTDBTableConfigTest {

  @Test
  void defaults_haveExpectedValues() {
    IoTDBTableConfig config = new IoTDBTableConfig();

    assertEquals("127.0.0.1", config.getHost());
    assertEquals(6667, config.getPort());
    assertEquals(8, config.getSessionPoolSize());
    assertEquals(-1L, config.getDefaultTtlMs());
    assertEquals("root", config.getUsername());
    assertEquals("root", config.getPassword());
    assertEquals(5000, config.getConnectionTimeoutMs());
    assertFalse(config.isEnableCompression());
  }

  @Test
  void binding_fromProperties_overridesDefaults() {
    MapConfigurationPropertySource source =
        new MapConfigurationPropertySource(
            Map.of(
                "iotdb.host", "10.0.0.5",
                "iotdb.port", "6668",
                "iotdb.session-pool-size", "16",
                "iotdb.default-ttl-ms", "86400000",
                "iotdb.username", "iot_user",
                "iotdb.password", "iot_pass",
                "iotdb.connection-timeout-ms", "9000",
                "iotdb.enable-compression", "true"));

    IoTDBTableConfig config = new Binder(source).bind("iotdb", IoTDBTableConfig.class).get();

    assertEquals("10.0.0.5", config.getHost());
    assertEquals(6668, config.getPort());
    assertEquals(16, config.getSessionPoolSize());
    assertEquals(86400000L, config.getDefaultTtlMs());
    assertEquals("iot_user", config.getUsername());
    assertEquals("iot_pass", config.getPassword());
    assertEquals(9000, config.getConnectionTimeoutMs());
    assertEquals(true, config.isEnableCompression());
  }
}
