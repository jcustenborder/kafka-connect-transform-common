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
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class ToJSONConfig extends AbstractConfig {
  public final Schema.Type outputSchema;
  public final boolean schemasEnable;

  public static final String OUTPUT_SCHEMA_CONFIG = "output.schema.type";
  public static final String OUTPUT_SCHEMA_DOC = "The connect schema type to output the converted JSON as.";
  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final String SCHEMAS_ENABLE_DOC = "Flag to determine if the JSON data should include the schema.";


  public ToJSONConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.outputSchema = ConfigUtils.getEnum(Schema.Type.class, this, OUTPUT_SCHEMA_CONFIG);
    this.schemasEnable = getBoolean(SCHEMAS_ENABLE_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(OUTPUT_SCHEMA_CONFIG, ConfigDef.Type.STRING)
                .documentation(OUTPUT_SCHEMA_DOC)
                .defaultValue(Schema.Type.STRING.toString())
                .validator(
                    ConfigDef.ValidString.in(
                        Schema.Type.STRING.toString(),
                        Schema.Type.BYTES.toString()
                    )
                )
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SCHEMAS_ENABLE_DOC)
                .defaultValue(false)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        );
  }

}
