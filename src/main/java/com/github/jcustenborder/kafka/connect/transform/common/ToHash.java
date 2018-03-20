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
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
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

public abstract class ToHash<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ToHash.class);
  ToHashConfig config;

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new ToHashConfig(map);
  }

  @Override
  public ConfigDef config() {
    return ToHashConfig.config();
  }

  @Override
  public void close() {

  }

  SchemaAndValue processHashCode(Schema inputSchema, HashCode hashCode) {
    final Schema schema = this.config.outputSchema(inputSchema.isOptional());
    final Object value;
    switch (this.config.outputType) {
      case STRING:
        value = hashCode.toString();
        break;
      case BYTES:
        value = hashCode.asBytes();
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                ToHashConfig.OUTPUT_TYPE_CONFIG + " of '%s' is not supported.",
                this.config.outputType
            )
        );
    }
    return new SchemaAndValue(schema, value);
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    final HashCode hashCode = this.config.hashFunction.hashString(input, Charsets.UTF_8);
    return processHashCode(inputSchema, hashCode);
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    final HashCode hashCode = this.config.hashFunction.hashBytes(input);
    return processHashCode(inputSchema, hashCode);
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
          fieldSchema = this.config.outputSchema(field.schema().isOptional());
        } else {
          fieldSchema = field.schema();
        }
        builder.field(field.name(), fieldSchema);
      }
      return builder.build();
    });

    Struct struct = new Struct(schema);
    for (Field field : inputSchema.fields()) {
      final Object value;
      if (this.config.fields.contains(field.name())) {
        log.trace("processStruct() - processing '{}'", field.name());
        if (null == input.get(field.name())) {
          value = null;
        } else {
          final HashCode hashCode;

          switch (field.schema().type()) {
            case STRING:
              hashCode = this.config.hashFunction.hashString(
                  input.getString(field.name()),
                  Charsets.UTF_8
              );
              break;
            case BYTES:
              hashCode = this.config.hashFunction.hashBytes(
                  input.getBytes(field.name())
              );
              break;
            default:
              throw new UnsupportedOperationException(
                  String.format("Problem hashing field(). %s:%s is not supported.",
                      field.name(),
                      field.schema().type(),
                      field.schema().name()
                  )
              );
          }

          final SchemaAndValue schemaAndValue = processHashCode(field.schema(), hashCode);
          value = schemaAndValue.value();
        }
      } else {
        value = input.get(field.name());
      }

      struct.put(field.name(), value);
    }
    return new SchemaAndValue(schema, struct);
  }

  @Title("ToHash(Key)")
  @Description("This transformation will hash the data with the specified algorithm.")
  public static class Key<R extends ConnectRecord<R>> extends ToHash<R> {

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

  @Title("ToHash(Value)")
  @Description("This transformation will hash the data with the specified algorithm.")
  public static class Value<R extends ConnectRecord<R>> extends ToHash<R> {

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
