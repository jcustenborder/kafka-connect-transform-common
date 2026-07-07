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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Title("HeaderToField")
@Description("This transformation is used to copy the value of a header to a field in the key or " +
    "value of the record.")
public abstract class HeaderToField<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(HeaderToField.class);

  HeaderToFieldConfig config;

  protected HeaderToField(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return HeaderToFieldConfig.config();
  }


  static class Conversion {
    public final Schema newSchema;
    public final List<ConversionHandler> conversionHandlers;

    private Conversion(Schema newSchema, List<ConversionHandler> conversionHandlers) {
      this.newSchema = newSchema;
      this.conversionHandlers = conversionHandlers;
    }

    public SchemaAndValue apply(ConnectRecord record, Struct input) {
      Struct result = new Struct(this.newSchema);
      for (Field field : input.schema().fields()) {
        String fieldName = field.name();
        Object fieldValue = input.get(field);
        result.put(fieldName, fieldValue);
      }
      for (ConversionHandler handler : this.conversionHandlers) {
        handler.convert(record, result);
      }
      return new SchemaAndValue(this.newSchema, result);
    }

    public static Conversion of(Schema newSchema, List<ConversionHandler> conversionHandlers) {
      return new Conversion(newSchema, conversionHandlers);
    }
  }


  Map<Schema, Conversion> schemaCache = new HashMap<>();


  Conversion conversion(Schema schema) {
    return this.schemaCache.computeIfAbsent(schema, s -> {
      log.info("conversion() - Building new schema for {}", schema);

      SchemaBuilder builder = SchemaBuilders.of(schema);
      List<ConversionHandler> handlers = new ArrayList<>(this.config.mappings.size());
      for (HeaderToFieldConfig.HeaderToFieldMapping mapping : this.config.mappings) {
        log.trace("conversion() - adding field '{}' with schema {}", mapping.field, mapping.schema);
        builder.field(mapping.field, mapping.schema);
        ConversionHandler handler = ConversionHandler.of(mapping.schema, mapping.header, mapping.field);
        handlers.add(handler);
      }
      Schema newSchema = builder.build();
      return Conversion.of(newSchema, handlers);
    });
  }


  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    Conversion conversion = conversion(inputSchema);
    return conversion.apply(record, input);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new HeaderToFieldConfig(map);
  }

  public static class Key<R extends ConnectRecord<R>> extends HeaderToField<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends HeaderToField<R> {
    public Value() {
      super(false);
    }
  }
}
