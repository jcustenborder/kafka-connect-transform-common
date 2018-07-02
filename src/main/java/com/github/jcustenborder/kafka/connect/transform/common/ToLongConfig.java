/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ToLongConfig extends AbstractConfig {
  public final Set<String> fields;

  public static final String FIELD_CONFIG = "fields";
  public static final String FIELD_DOC = "The fields to transform.";


  public ToLongConfig(Map<String, ?> settings) {
    super(config(), settings);
    List<String> fields = getList(FIELD_CONFIG);
    this.fields = new HashSet<>(fields);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(FIELD_CONFIG, ConfigDef.Type.LIST)
                .documentation(FIELD_DOC)
                .defaultValue(Collections.emptyList())
                .importance(ConfigDef.Importance.HIGH)
                .build()
        );
  }

}
