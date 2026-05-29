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

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * Configuration properties for the IoTDB Table Mode DAO backend. Bound from {@code iotdb.*} in
 * Spring application config.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "iotdb")
public class IoTDBTableConfig {

  @NotBlank private String host = "127.0.0.1";

  @Min(1)
  @Max(65535)
  private int port = 6667;

  @Min(1)
  @Max(1024)
  private int sessionPoolSize = 8;

  @Min(-1)
  private long defaultTtlMs = -1L;

  @NotBlank private String username = "root";

  @NotBlank private String password = "root";

  @Min(100)
  private int connectionTimeoutMs = 5000;

  private boolean enableCompression = false;
}
