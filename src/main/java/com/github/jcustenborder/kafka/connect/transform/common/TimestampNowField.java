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
import com.github.jcustenborder.kafka.connect.utils.data.SchemaBuilders;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Title("TimestampNowField")
@Description("This transformation is used to set a field with the current timestamp of the system running the " +
    "transformation.")
public abstract class TimestampNowField<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  private TimestampNowFieldConfig config;
  Time time = Time.SYSTEM;

  protected TimestampNowField(boolean isKey) {
    super(isKey);
  }

  public static class Key<R extends ConnectRecord<R>> extends TimestampNowField<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends TimestampNowField<R> {
    public Value() {
      super(false);
    }
  }

  @Override
  public void close() {

  }

  Map<Schema, Schema> schemaCache = new HashMap<>();

  static boolean isTimestampSchema(Schema schema) {
    return (Timestamp.SCHEMA.type() == schema.type() && Timestamp.SCHEMA.name().equals(schema.name()));
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    Date timestamp = new Date(this.time.milliseconds());

    Schema outputSchema = schemaCache.computeIfAbsent(inputSchema, schema -> {
      Collection<String> replaceFields = schema.fields().stream()
          .filter(f -> this.config.fields.contains(f.name()))
          .filter(f -> !isTimestampSchema(f.schema()))
          .map(Field::name)
          .collect(Collectors.toList());
      SchemaBuilder builder = SchemaBuilders.of(schema, replaceFields);
      this.config.fields.forEach(timestampField -> {
        Field existingField = builder.field(timestampField);
        if (null == existingField) {
          builder.field(timestampField, Timestamp.SCHEMA);
        }
      });
      return builder.build();
    });

    Struct output = new Struct(outputSchema);
    inputSchema.fields().stream()
        .filter(f -> !this.config.fields.contains(f.name()))
        .forEach(f -> output.put(f.name(), input.get(f.name())));
    this.config.fields.forEach(field -> output.put(field, timestamp));
    return new SchemaAndValue(outputSchema, output);
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    Map<String, Object> result = new LinkedHashMap<>(input);
    Date timestamp = new Date(this.time.milliseconds());
    this.config.fields.forEach(field -> result.put(field, timestamp));
    return new SchemaAndValue(null, result);
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new TimestampNowFieldConfig(settings);
  }

  @Override
  public ConfigDef config() {
    return TimestampNowFieldConfig.config();
  }
}
