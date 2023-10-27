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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class ChangeCase<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ChangeCase.class);

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

  Map<Schema, Schema> schemaState = new HashMap<>();

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final Schema outputSchema = this.schemaState.computeIfAbsent(inputSchema, schema -> convertSchema(schema));
    final Struct outputStruct = convertStruct(inputSchema, outputSchema, input);
    return new SchemaAndValue(outputSchema, outputStruct);
  }

  private Struct convertStruct(Schema inputSchema, Schema outputSchema, Struct input) {
    final Struct struct = new Struct(outputSchema);

    if (input == null)
      return struct;

    for (Field inputField : inputSchema.fields()) {
      final int index = inputField.index();
      final Field outputField = outputSchema.fields().get(index);
      final Schema inputFieldSchema = inputField.schema();
      final Schema outputFieldSchema = outputField.schema();
      final Object value = convertValue(inputFieldSchema, outputFieldSchema, input.get(inputField));
      struct.put(outputField, value);
    }
    return struct;
  }

  private Object convertValue(Schema inputFieldSchema, Schema outputFieldSchema, Object value) {
    switch (outputFieldSchema.type()) {
      case STRUCT: {
        return convertStruct(inputFieldSchema, outputFieldSchema, (Struct) value);
      }
      case ARRAY: {
        return convertArray(inputFieldSchema, outputFieldSchema, (List<Object>) value);
      }
    }
    return value;
  }

  private Object convertArray(Schema inputFieldSchema, Schema outputFieldSchema, List<Object> value) {
    final Schema inputSchema = inputFieldSchema.valueSchema();
    final Schema outputSchema = outputFieldSchema.valueSchema();
    switch (outputSchema.type()) {
      case STRUCT: {
        return value.stream().map(entry -> convertStruct(
                inputSchema,
                outputSchema,
                (Struct) entry
        )).collect(Collectors.toList());
      }
      case ARRAY: {
        return value.stream().map(entry -> convertArray(
                inputSchema,
                outputSchema,
                (List<Object>) entry
        )).collect(Collectors.toList());
      }
    }
    return value;
  }

  private Schema convertSchema(Schema inputSchema) {
    switch (inputSchema.type()) {
      case ARRAY: {
        log.trace("convertSchema() - Recurse into array");
        final SchemaBuilder builder = SchemaBuilder.array(convertSchema(inputSchema.valueSchema()));
        if (inputSchema.isOptional()) {
          builder.optional();
        }
        return builder.build();
      }
      case STRUCT: {
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (!Strings.isNullOrEmpty(inputSchema.name())) {
          builder.name(inputSchema.name());
        }
        if (inputSchema.isOptional()) {
          builder.optional();
        }
        for (Field field : inputSchema.fields()) {
          final String newFieldName = this.config.from.to(this.config.to, field.name());
          log.trace("convertSchema() - Mapped '{}' to '{}'", field.name(), newFieldName);
          builder.field(newFieldName, convertSchema(field.schema()));
        }
        return builder.build();
      }
    }
    return inputSchema;
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
