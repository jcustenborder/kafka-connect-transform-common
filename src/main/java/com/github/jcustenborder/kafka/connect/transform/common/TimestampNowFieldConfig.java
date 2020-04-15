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
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

class TimestampNowFieldConfig extends AbstractConfig {
  public static final String FIELDS_CONF = "fields";
  public static final String FIELDS_DOC = "The field(s) that will be inserted with the timestamp of the system.";

  public final Set<String> fields;

  public TimestampNowFieldConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fields = ConfigUtils.getSet(this, FIELDS_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(FIELDS_CONF, ConfigDef.Type.LIST)
                .documentation(FIELDS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        );
  }
}
