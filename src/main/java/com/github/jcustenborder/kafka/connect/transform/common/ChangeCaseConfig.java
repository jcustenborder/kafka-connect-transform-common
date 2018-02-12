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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.base.CaseFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class ChangeCaseConfig extends AbstractConfig {
  public final CaseFormat from;
  public final CaseFormat to;

  public static final String FROM_CONFIG = "from";
  static final String FROM_DOC = "The format to move from ";
  public static final String TO_CONFIG = "to";
  static final String TO_DOC = "";

  public ChangeCaseConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.from = ConfigUtils.getEnum(CaseFormat.class, this, FROM_CONFIG);
    this.to = ConfigUtils.getEnum(CaseFormat.class, this, TO_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FROM_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ValidEnum.of(CaseFormat.class), ConfigDef.Importance.HIGH, FROM_DOC)
        .define(TO_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ValidEnum.of(CaseFormat.class), ConfigDef.Importance.HIGH, TO_DOC);
  }
}
