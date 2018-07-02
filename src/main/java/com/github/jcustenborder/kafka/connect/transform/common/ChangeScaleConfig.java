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
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.math.RoundingMode;
import java.util.Map;
import java.util.Set;

class ChangeScaleConfig extends AbstractConfig {
  public final int scale;
  public final RoundingMode roundingMode;
  public final Set<String> fields;

  public static final String SCALE_CONFIG = "scale";
  static final String SCALE_DOC = "";
  public static final String ROUNDING_MODE_CONFIG = "rounding.mode";
  static final String ROUNDING_MODE_DOC = "";
  public static final String FIELDS_CONFIG = "fields";
  public static final String FIELDS_DOC = "";

  public ChangeScaleConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.scale = getInt(SCALE_CONFIG);
    this.roundingMode = ConfigUtils.getEnum(RoundingMode.class, this, ROUNDING_MODE_CONFIG);
    this.fields = ImmutableSet.copyOf(getList(FIELDS_CONFIG));
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(SCALE_CONFIG, ConfigDef.Type.INT)
                .documentation(SCALE_DOC)
                .validator(ConfigDef.Range.atLeast(1))
                .build()
        ).define(
            ConfigKeyBuilder.of(ROUNDING_MODE_CONFIG, ConfigDef.Type.STRING)
                .documentation(ROUNDING_MODE_DOC)
                .defaultValue(RoundingMode.HALF_UP.toString())
                .validator(ValidEnum.of(RoundingMode.class))
                .build()
        ).define(
            ConfigKeyBuilder.of(FIELDS_CONFIG, ConfigDef.Type.LIST)
                .documentation(FIELDS_DOC)
                .build()
        );
  }
}
