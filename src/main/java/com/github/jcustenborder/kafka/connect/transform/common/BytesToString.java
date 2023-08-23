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
import java.util.Map;

public abstract class BytesToString<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(BytesToString.class);

  @Override
  public ConfigDef config() {
    return BytesToStringConfig.config();
  }

  BytesToStringConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new BytesToStringConfig(settings);
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    final Schema outputSchema = inputSchema.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
    final String output =  input != null
            ?  new String(input, this.config.charset)
            : (String) inputSchema.defaultValue();
    return new SchemaAndValue(outputSchema, output);
  }

  Map<Schema, Schema> schemaCache = new HashMap<>();

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final Schema schema = this.schemaCache.computeIfAbsent(inputSchema, s -> {
      final SchemaBuilder builder = SchemaBuilder.struct();
      if (!Strings.isNullOrEmpty(inputSchema.name())) {
        builder.name(inputSchema.name());
      }
      if (inputSchema.isOptional()) {
        builder.optional();
      }

      for (Field field : inputSchema.fields()) {
        log.trace("processStruct() - processing '{}'", field.name());
        final Schema fieldSchema;
        if (this.config.fields.contains(field.name())) {
          fieldSchema = field.schema().isOptional() ?
              Schema.OPTIONAL_STRING_SCHEMA :
              Schema.STRING_SCHEMA;
        } else {
          fieldSchema = field.schema();
        }
        builder.field(field.name(), fieldSchema);
      }
      return builder.build();
    });

    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      if (this.config.fields.contains(field.name())) {
        byte[] buffer = input.getBytes(field.name());
        struct.put(
                field.name(),
                buffer != null
                  ?  new String(buffer, this.config.charset)
                : field.schema().defaultValue()
        );
      } else {
        struct.put(field.name(), input.get(field.name()));
      }
    }
    return new SchemaAndValue(schema, struct);
  }

  @Title("BytesToString(Key)")
  @Description("This transformation is used to convert a byte array to a string.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends BytesToString<R> {

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

  @Title("BytesToString(Value)")
  @Description("This transformation is used to convert a byte array to a string.")
  public static class Value<R extends ConnectRecord<R>> extends BytesToString<R> {
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
