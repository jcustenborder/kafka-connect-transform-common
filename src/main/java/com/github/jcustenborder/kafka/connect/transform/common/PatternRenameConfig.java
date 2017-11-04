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

class PatternRenameConfig extends AbstractConfig {

  public static final String FIELD_PATTERN_CONF = "field.pattern";
  static final String FIELD_PATTERN_DOC = "";

  public static final String FIELD_PATTERN_FLAGS_CONF = "field.pattern.flags";
  static final String FIELD_PATTERN_FLAGS_DOC = "";

  public static final String FIELD_REPLACEMENT_CONF = "field.replacement";
  static final String FIELD_REPLACEMENT_DOC = "";


  public final Pattern pattern;
  public final String replacement;

  public PatternRenameConfig(Map<String, ?> parsedConfig) {
    super(config(), parsedConfig);
    final String pattern = getString(FIELD_PATTERN_CONF);
    final List<String> flagList = getList(FIELD_PATTERN_FLAGS_CONF);
    int patternFlags = 0;
    for (final String f : flagList) {
      final int flag = FLAG_VALUES.get(f);
      patternFlags = patternFlags | flag;
    }
    this.pattern = Pattern.compile(pattern, patternFlags);
    this.replacement = getString(FIELD_REPLACEMENT_CONF);
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
        .define(FIELD_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FIELD_PATTERN_DOC)
        .define(FIELD_PATTERN_FLAGS_CONF, ConfigDef.Type.LIST, Arrays.asList("CASE_INSENSITIVE"), ConfigDef.ValidList.in("UNICODE_CHARACTER_CLASS", "CANON_EQ", "UNICODE_CASE", "DOTALL", "LITERAL", "MULTILINE", "COMMENTS", "CASE_INSENSITIVE", "UNIX_LINES"), ConfigDef.Importance.LOW, FIELD_PATTERN_FLAGS_DOC)
        .define(FIELD_REPLACEMENT_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FIELD_REPLACEMENT_DOC);
  }
}
