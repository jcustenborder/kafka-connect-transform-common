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

public abstract class ExtractNestedField<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ExtractNestedField.class);


  @Override
  public ConfigDef config() {
    return ExtractNestedFieldConfig.config();
  }

  @Override
  public void close() {

  }

  ExtractNestedFieldConfig config;
  Map<Schema, Schema> schemaCache;

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new ExtractNestedFieldConfig(map);
    this.schemaCache = new HashMap<>();
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final Struct innerStruct = input.getStruct(this.config.outerFieldName);
    final Schema outputSchema = this.schemaCache.computeIfAbsent(inputSchema, s -> {

      final Field outerField = inputSchema.field(this.config.outerFieldName);
      final Field innerField = outerField.schema().field(this.config.innerFieldName);
      final Schema innerFieldSchema = outerField.schema().isOptional()
          ? optional(innerField.schema())
          : innerField.schema();
      final SchemaBuilder builder = SchemaBuilder.struct();
      if (!Strings.isNullOrEmpty(inputSchema.name())) {
        builder.name(inputSchema.name());
      }
      if (inputSchema.isOptional()) {
        builder.optional();
      }
      for (Field inputField : inputSchema.fields()) {
        builder.field(inputField.name(), inputField.schema());
      }
      builder.field(this.config.outputFieldName, innerFieldSchema);
      return builder.build();
    });
    final Struct outputStruct = new Struct(outputSchema);
    for (Field inputField : inputSchema.fields()) {
      final Object value = input.get(inputField);
      outputStruct.put(inputField.name(), value);
    }
    final Object innerFieldValue = null == innerStruct ? null : innerStruct.get(this.config.innerFieldName);
    outputStruct.put(this.config.outputFieldName, innerFieldValue);

    return new SchemaAndValue(outputSchema, outputStruct);

  }

  private static Schema optional(Schema schema) {
    if (schema.isOptional()) {
      return schema;
    }

    final SchemaBuilder builder;
    switch (schema.type()) {
      case ARRAY:
        builder = SchemaBuilder.array(schema.valueSchema());
        break;
      case MAP:
        builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
        break;
      case STRUCT:
        builder = SchemaBuilder.struct();
        for (Field field : schema.fields()) {
          builder.field(field.name(), field.schema());
        }
        break;
      default:
        builder = SchemaBuilder.type(schema.type());
    }
    if (!Strings.isNullOrEmpty(schema.name())) {
      builder.name(schema.name());
    }
    if (null != schema.version()) {
      builder.version(schema.version());
    }
    if (!Strings.isNullOrEmpty(schema.doc())) {
      builder.doc(schema.doc());
    }
    if (null != schema.parameters() && !schema.parameters().isEmpty()) {
      builder.parameters(schema.parameters());
    }
    if (null != schema.defaultValue()) {
      builder.defaultValue(schema.defaultValue());
    }
    return builder.optional().build();
  }


  @Title("ExtractNestedField(Key)")
  @Description("This transformation is used to extract a field from a nested struct and append it " +
      "to the parent struct.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends ExtractNestedField<R> {

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

  @Title("ExtractNestedField(Value)")
  @Description("This transformation is used to extract a field from a nested struct and append it " +
      "to the parent struct.")
  public static class Value<R extends ConnectRecord<R>> extends ExtractNestedField<R> {

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
