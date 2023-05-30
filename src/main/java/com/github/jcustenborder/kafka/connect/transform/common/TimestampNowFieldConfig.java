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
import com.github.jcustenborder.kafka.connect.utils.config.validators.ValidChronoUnit;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

class TimestampNowFieldConfig extends AbstractConfig {
  public static final String FIELDS_CONF = "fields";
  public static final String ADD_AMOUNT_CONF = "add.amount";
  public static final String ADD_CHRONO_UNIT_CONF = "add.chronounit";

  public static final String TARGET_TYPE_CONF = "target.type";

  public static final String FIELDS_DOC = "The field(s) that will be inserted with the timestamp of the system.";

  public static final String ADD_AMOUNT_DOC = "how many of the chosen ChronoUnits to add on top of the timestamp of the system.";

  public static final String ADD_CHRONO_UNIT_DOC = "String representation of the ChronoUnit to add, eg: 'DAYS'";

  public static final String TARGET_TYPE_DOC = "The desired timestamp representation: Unix, Date";

  public final Set<String> fields;
  public final Long addAmount;
  public final ChronoUnit addChronoUnit;

  public final TimestampNowFieldTargetType targetType;

  public TimestampNowFieldConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fields = ConfigUtils.getSet(this, FIELDS_CONF);
    this.addAmount = getLong(ADD_AMOUNT_CONF);
    this.addChronoUnit = ChronoUnit.valueOf(getString(ADD_CHRONO_UNIT_CONF));
    this.targetType = ConfigUtils.getEnum(TimestampNowFieldTargetType.class, this, TARGET_TYPE_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(FIELDS_CONF, ConfigDef.Type.LIST)
                .documentation(FIELDS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(ADD_AMOUNT_CONF, ConfigDef.Type.LONG)
                .documentation(ADD_AMOUNT_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue("0")
                .build()
        ).define(
            ConfigKeyBuilder.of(ADD_CHRONO_UNIT_CONF, ConfigDef.Type.STRING)
                .documentation(ADD_CHRONO_UNIT_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue("DAYS")
                .validator(new ValidChronoUnit())
                .build()
        ).define(
            ConfigKeyBuilder.of(TARGET_TYPE_CONF, ConfigDef.Type.STRING)
                .documentation(TARGET_TYPE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue("Date")
                .validator(Validators.validEnum(TimestampNowFieldTargetType.class))
                .build()
        );
  }
}
