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

public class AdjustPrecisionAndScaleConfig extends AbstractConfig {
  public final int precision;
  public final int scale;
  public final String precisionMode;
  public final String scaleMode;
  public final String scaleNegativeMode;
  public final int undefinedScaleValue;

  public AdjustPrecisionAndScaleConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.precision = getInt(PRECISION_VALUE_CONFIG);
    this.scale = getInt(SCALE_VALUE_CONFIG);
    this.precisionMode = getString(PRECISION_MODE_CONFIG);
    this.scaleMode = getString(SCALE_MODE_CONFIG);
    this.scaleNegativeMode = getString(SCALE_NEGATIVE_MODE_CONFIG);
    this.undefinedScaleValue = getInt(SCALE_UNDEFINED_VALUE_CONFIG);
  }

  public static final String PRECISION_VALUE_CONFIG = "precision.value";
  static final String PRECISION_VALUE_DOC = "Precision to use for precision modification (default is 38).";

  public static final String PRECISION_MODE_CONFIG = "precision.mode";
  static final String PRECISION_MODE_DOC = "Mode to use for precision modification:\n" +
          "'none' (default): Perform no modification\n" +
          "'undefined': Use provided precision when precision is undefined\n" +
          "'max': Use provided precision as max precision";

  public static final String PRECISION_MODE_NONE = "none";
  public static final String PRECISION_MODE_UNDEFINED = "undefined";
  public static final String PRECISION_MODE_MAX = "max";

  public static final String SCALE_VALUE_CONFIG = "scale.value";
  static final String SCALE_VALUE_DOC = "Scale to use for scale modification (default is 127).";

  // 'scale.mode' and 'negative.scale.mode' could be a single property but it's harder to reason about (9 combinations)
  // Note that a scale cannot both be both negative and undefined, or negative and higher than the (positive) max value
  public static final String SCALE_MODE_CONFIG = "scale.mode";
  static final String SCALE_MODE_DOC = "Mode to use for scale modification:\n" +
          "'none' (default): Perform no modification\n" +
          "'undefined': Use provided scale when scale (and precision) are undefined\n" +
          "'max': Use provided scale as max scale, or when scale (and precision) are undefined\n";

  public static final String SCALE_MODE_NONE = "none";
  public static final String SCALE_MODE_UNDEFINED = "undefined";
  public static final String SCALE_MODE_MAX = "max";

  public static final String SCALE_NEGATIVE_MODE_CONFIG = "scale.negative.mode";
  public static final String SCALE_NEGATIVE_MODE_DOC = "Mode for handling negative scale:\n" +
          "'none' (default): Perform no modification\n" +
          "'zero': Set to zero\n" +
          "'value': Set to provided scale";

  public static final String SCALE_NEGATIVE_MODE_NONE = "none";
  public static final String SCALE_NEGATIVE_MODE_ZERO = "zero";
  public static final String SCALE_NEGATIVE_MODE_VALUE = "value";

  public static final String SCALE_UNDEFINED_VALUE_CONFIG = "scale.undefined.value";
  static final String SCALE_UNDEFINED_VALUE_DOC = "JDBC Source Connectors report undefined scale as 127;" +
          "use this to specify another value to detect as 'undefined' scale.";
  static final int SCALE_UNDEFINED_VALUE_DEFAULT = 127;

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(PRECISION_VALUE_CONFIG, ConfigDef.Type.INT)
                .documentation(PRECISION_VALUE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(38)
                .validator(ConfigDef.Range.between(1, 64))
                .build()
        )
        .define(
              ConfigKeyBuilder.of(PRECISION_MODE_CONFIG, ConfigDef.Type.STRING)
                .documentation(PRECISION_MODE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(PRECISION_MODE_NONE)
                .validator(ConfigDef.ValidString.in(
                    PRECISION_MODE_NONE,
                    PRECISION_MODE_UNDEFINED,
                    PRECISION_MODE_MAX
                ))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCALE_VALUE_CONFIG, ConfigDef.Type.INT)
                .documentation(SCALE_VALUE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(127)
                .validator(ConfigDef.Range.between(0, 127))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCALE_MODE_CONFIG, ConfigDef.Type.STRING)
                .documentation(SCALE_MODE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(SCALE_MODE_NONE)
                .validator(ConfigDef.ValidString.in(
                    SCALE_MODE_NONE,
                    SCALE_MODE_UNDEFINED,
                    SCALE_MODE_MAX
                ))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCALE_NEGATIVE_MODE_CONFIG, ConfigDef.Type.STRING)
                    .documentation(SCALE_NEGATIVE_MODE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(SCALE_NEGATIVE_MODE_NONE)
                .validator(ConfigDef.ValidString.in(
                    SCALE_NEGATIVE_MODE_NONE,
                    SCALE_NEGATIVE_MODE_ZERO,
                    SCALE_NEGATIVE_MODE_VALUE
                ))
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCALE_UNDEFINED_VALUE_CONFIG, ConfigDef.Type.INT)
                .documentation(SCALE_UNDEFINED_VALUE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(SCALE_UNDEFINED_VALUE_DEFAULT)
                .build()
        );
  }
}
