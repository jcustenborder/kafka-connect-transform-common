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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class ExtractTimestampConfig extends AbstractConfig {
  public final String fieldName;

  public static final String FIELD_NAME_CONFIG = "field.name";
  public static final String FIELD_NAME_DOC = "The field to pull the timestamp from. This must be an int64 or a timestamp.";

  public ExtractTimestampConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fieldName = getString(FIELD_NAME_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FIELD_NAME_DOC);
  }
}
