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

import java.util.Map;

public class SetMaximumPrecisionConfig extends AbstractConfig {
  public final int maxPrecision;

  public SetMaximumPrecisionConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.maxPrecision = getInt(MAX_PRECISION_CONFIG);
  }

  public static final String MAX_PRECISION_CONFIG = "precision.max";
  static final String MAX_PRECISION_DOC = "The maximum precision allowed.";

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(MAX_PRECISION_CONFIG, ConfigDef.Type.INT)
                .documentation(MAX_PRECISION_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(ConfigDef.Range.between(1, 64))
                .build()
        );
  }
}
