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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

class PatternMapStringConfig extends AbstractConfig {

  public static final String SRC_FIELD_NAME_CONF = "src.field.name";
  static final String SRC_FIELD_NAME_DOC = "The name of the input field";

  public static final String DEST_FIELD_NAME_CONF = "dest.field.name";
  static final String DEST_FIELD_NAME_DOC = "The name of the output field, set to the same as 'src.field.name' to replace a field value";

  public static final String VALUE_PATTERN_CONF = "value.pattern";
  static final String VALUE_PATTERN_DOC = "RegEx pattern which will be replaced";

  public static final String VALUE_PATTERN_FLAGS_CONF = "value.pattern.flags";
  static final String VALUE_PATTERN_FLAGS_DOC = "";

  public static final String VALUE_REPLACEMENT_CONF = "value.replacement";
  static final String VALUE_REPLACEMENT_DOC = "RegEx to generate output for each match";


  public final Pattern pattern;
  public final String replacement;
  public final String srcfieldname;
  public final String destfieldname;

  public PatternMapStringConfig(Map<String, ?> parsedConfig) {
    super(config(), parsedConfig);
    final String pattern = getString(VALUE_PATTERN_CONF);
    final List<String> flagList = getList(VALUE_PATTERN_FLAGS_CONF);
    int patternFlags = 0;
    for (final String f : flagList) {
      final int flag = FLAG_VALUES.get(f);
      patternFlags = patternFlags | flag;
    }
    this.pattern = Pattern.compile(pattern, patternFlags);
    this.replacement = getString(VALUE_REPLACEMENT_CONF);
    this.srcfieldname = getString(SRC_FIELD_NAME_CONF);
    this.destfieldname = getString(DEST_FIELD_NAME_CONF);
  }

  static final Map<String, Integer> FLAG_VALUES;

  static {
    Map<String, Integer> map = new HashMap<>();
    map.put("UNICODE_CHARACTER_CLASS", 0x100);
    map.put("CANON_EQ", 0x80);
    map.put("UNICODE_CASE", 0x40);
    map.put("DOTALL", 0x20);
    map.put("LITERAL", 0x10);
    map.put("MULTILINE", 0x08);
    map.put("COMMENTS", 0x04);
    map.put("CASE_INSENSITIVE", 0x02);
    map.put("UNIX_LINES", 0x01);
    FLAG_VALUES = ImmutableMap.copyOf(map);
  }

  public static ConfigDef config() {


    return new ConfigDef()
        .define(VALUE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, VALUE_PATTERN_DOC)
        .define(VALUE_PATTERN_FLAGS_CONF, ConfigDef.Type.LIST, Arrays.asList("CASE_INSENSITIVE"), ConfigDef.ValidList.in("UNICODE_CHARACTER_CLASS", "CANON_EQ", "UNICODE_CASE", "DOTALL", "LITERAL", "MULTILINE", "COMMENTS", "CASE_INSENSITIVE", "UNIX_LINES"), ConfigDef.Importance.LOW, VALUE_PATTERN_FLAGS_DOC)
        .define(VALUE_REPLACEMENT_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, VALUE_REPLACEMENT_DOC)
        .define(SRC_FIELD_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SRC_FIELD_NAME_DOC)
        .define(DEST_FIELD_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DEST_FIELD_NAME_DOC);
  }
}
