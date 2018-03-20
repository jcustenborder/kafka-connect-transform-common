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
import com.google.common.base.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class ChangeCase<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ChangeCase.class);

  class State {
    public final Map<String, String> columnMapping;
    public final Schema schema;

    State(Map<String, String> columnMapping, Schema schema) {
      this.columnMapping = columnMapping;
      this.schema = schema;
    }
  }

  private ChangeCaseConfig config;

  @Override
  public ConfigDef config() {
    return ChangeCaseConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new ChangeCaseConfig(map);
  }

  Map<Schema, State> schemaState = new HashMap<>();

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final State state = this.schemaState.computeIfAbsent(inputSchema, schema -> {
      final SchemaBuilder builder = SchemaBuilder.struct();
      if (!Strings.isNullOrEmpty(schema.name())) {
        builder.name(schema.name());
      }
      if (schema.isOptional()) {
        builder.optional();
      }

      final Map<String, String> columnMapping = new LinkedHashMap<>();

      for (Field field : schema.fields()) {
        final String newFieldName = this.config.from.to(this.config.to, field.name());
        log.trace("processStruct() - Mapped '{}' to '{}'", field.name(), newFieldName);
        columnMapping.put(field.name(), newFieldName);
        builder.field(newFieldName, field.schema());
      }

      return new State(columnMapping, builder.build());
    });

    final Struct outputStruct = new Struct(state.schema);

    for (Map.Entry<String, String> kvp : state.columnMapping.entrySet()) {
      final Object value = input.get(kvp.getKey());
      outputStruct.put(kvp.getValue(), value);
    }

    return new SchemaAndValue(state.schema, outputStruct);
  }

  @Title("ChangeCase(Key)")
  @Description("This transformation is used to change the case of fields in an input struct.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends ChangeCase<R> {

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

  @Title("ChangeCase(Value)")
  @Description("This transformation is used to change the case of fields in an input struct.")
  public static class Value<R extends ConnectRecord<R>> extends ChangeCase<R> {

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
