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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

public abstract class PatternMapString<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(PatternMapString.class);

  @Override
  public ConfigDef config() {
    return PatternMapStringConfig.config();
  }

  PatternMapStringConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new PatternMapStringConfig(settings);
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    Schema outPutSchema = inputSchema;
    SchemaAndValue retVal = null;
    if (!config.destfieldname.equals(config.srcfieldname)) {
      // Adding a new field, need to build a new schema
      final SchemaBuilder outputSchemaBuilder = SchemaBuilder.struct();
      outputSchemaBuilder.name(inputSchema.name());
      outputSchemaBuilder.doc(inputSchema.doc());
      if (null != inputSchema.defaultValue()) {
        outputSchemaBuilder.defaultValue(inputSchema.defaultValue());
      }
      if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
        outputSchemaBuilder.parameters(inputSchema.parameters());
      }
      if (inputSchema.isOptional()) {
        outputSchemaBuilder.optional();
      }
      for (final Field inputField : inputSchema.fields()) {
        final String inputFieldName = inputField.name();
        log.trace("process() - Schema has field '{}'", inputFieldName);
        outputSchemaBuilder.field(inputFieldName, inputField.schema());
        if (inputFieldName.equals(config.srcfieldname)) {
          log.trace("process() - Adding field '{}'", config.destfieldname);
          outputSchemaBuilder.field(config.destfieldname, inputField.schema());
        }
      }
      final Schema newSchema = outputSchemaBuilder.build();
      final Struct outputStruct = new Struct(newSchema);
      for (final Field inputField : inputSchema.fields()) {
        final String inputFieldName = inputField.name();
        final Object value = inputStruct.get(inputFieldName);
        outputStruct.put(inputFieldName, value);
        if (inputFieldName.equals(config.srcfieldname)) {
          String replacedField = (String) value;
          final Matcher fieldMatcher = this.config.pattern.matcher(replacedField);
          String replacedValue = fieldMatcher.replaceAll(this.config.replacement);
          outputStruct.put(config.destfieldname, replacedValue);
        }
      }
      retVal = new SchemaAndValue(newSchema, outputStruct);
    } else {
      Struct outputStruct = inputStruct;
      Object toReplace = inputStruct.get(config.srcfieldname);
      if (toReplace != null && toReplace instanceof String) {
        String inputFieldName = config.srcfieldname;
        String replacedField = (String) toReplace;
        log.trace("process() - Processing struct field '{}' value '{}'", inputFieldName, toReplace);
        final Matcher fieldMatcher = this.config.pattern.matcher(replacedField);
        String replacedValue = fieldMatcher.replaceAll(this.config.replacement);
        if (config.destfieldname.equals(config.srcfieldname)) {
          log.debug("process() - Replaced struct field '{}' with '{}'", inputFieldName, replacedValue);
        } else {
          log.debug("process() - Added struct field '{}' with '{}'", config.destfieldname, replacedValue);
        }
        outputStruct.put(config.destfieldname, replacedValue);
        retVal = new SchemaAndValue(outPutSchema, outputStruct);
      }
    }
    return retVal;
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    Map<String, Object> outputMap = new LinkedHashMap<>(input.size());
    for (final String inputFieldName : input.keySet()) {
      outputMap.put(inputFieldName, input.get(inputFieldName));
      log.trace("process() - Processing map field '{}' value '{}'", inputFieldName, input.get(inputFieldName));
      if (inputFieldName.equals(config.srcfieldname)) {
        String fieldToMatch = (String) input.get(inputFieldName);
        final Matcher fieldMatcher = this.config.pattern.matcher(fieldToMatch);
        String replacedValue = fieldMatcher.replaceAll(this.config.replacement);
        outputMap.put(config.destfieldname, replacedValue);
        if (config.destfieldname.equals(config.srcfieldname)) {
          log.debug("process() - Replaced map field '{}' with '{}'", inputFieldName, replacedValue);
        } else {
          log.debug("process() - Added map field '{}' with '{}'", config.destfieldname, replacedValue);
        }
      }
    }
    return new SchemaAndValue(null, outputMap);
  }

  @Title("PatternReplace(Key)")
  @Description("This transformation is used to map the value of a field in the key of an input struct based on a regular expression and a replacement string.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends PatternMapString<R> {

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
  @Description("This transformation is used to map the value of a field in the value of an input struct based on a regular expression and a replacement string.")
  public static class Value<R extends ConnectRecord<R>> extends PatternMapString<R> {
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
