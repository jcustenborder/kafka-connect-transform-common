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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

public abstract class PatternReplace<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(PatternReplace.class);

  @Override
  public ConfigDef config() {
    return PatternReplaceConfig.config();
  }

  PatternReplaceConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new PatternReplaceConfig(settings);
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    Struct outputStruct = inputStruct;
    Object toReplace = inputStruct.get(config.fieldname);
    if (toReplace != null && toReplace instanceof String) {
      String replacedField = (String) toReplace;
      final Matcher fieldMatcher = this.config.pattern.matcher(replacedField);
      String replacedValue = fieldMatcher.replaceAll(this.config.replacement);
      outputStruct.put(config.fieldname, replacedValue);
    }
    return new SchemaAndValue(inputSchema, outputStruct);
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    Map<String, Object> outputMap = new LinkedHashMap<>(input.size());
    for (final String inputFieldName : input.keySet()) {
      log.trace("process() - Processing field '{}' value '{}'", inputFieldName, input.get(inputFieldName));
      if (inputFieldName.equals(config.fieldname)) {
        String fieldToMatch = (String) input.get(inputFieldName);
        final Matcher fieldMatcher = this.config.pattern.matcher(fieldToMatch);
        String replacedValue = fieldMatcher.replaceAll(this.config.replacement);
        outputMap.put(config.fieldname, replacedValue);
        log.debug("process() - Replaced field '{}' with '{}'", inputFieldName, replacedValue);
      } else {
        outputMap.put(inputFieldName, input.get(inputFieldName));
      }
    }
    return new SchemaAndValue(null, outputMap);
  }

  @Title("PatternReplace(Key)")
  @Description("This transformation is used to replace the value of a field in the key of an input struct based on a regular expression and a replacement string.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends PatternReplace<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("PatternReplace(Value)")
  @Description("This transformation is used to replace the value of a fields in the value of an input struct based on a regular expression and a replacement string.")
  public static class Value<R extends ConnectRecord<R>> extends PatternReplace<R> {
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }
}
