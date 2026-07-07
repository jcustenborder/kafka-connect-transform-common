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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Title("SetMaximumPrecision")
@Description("This transformation is used to ensure that all decimal fields in a struct are below the " +
    "maximum precision specified.")
@DocumentationNote("The Confluent AvroConverter uses a default precision of 64 which can be too large " +
    "for some database systems.")
public abstract class SetMaximumPrecision<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(SetMaximumPrecision.class);

  public SetMaximumPrecision(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return SetMaximumPrecisionConfig.config();
  }

  @Override
  public void close() {

  }

  SetMaximumPrecisionConfig config;

  static final State NOOP = new State(true, null, null);

  static class State {
    public final boolean noop;
    public final Schema outputSchema;
    public final Set<String> decimalFields;

    State(boolean noop, Schema outputSchema, Set<String> decimalFields) {
      this.noop = noop;
      this.outputSchema = outputSchema;
      this.decimalFields = decimalFields;
    }
  }

  Map<Schema, State> schemaLookup = new HashMap<>();

  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

  State state(Schema inputSchema) {
    return this.schemaLookup.computeIfAbsent(inputSchema, new Function<Schema, State>() {
      @Override
      public State apply(Schema schema) {
        Set<String> decimalFields = inputSchema.fields().stream()
            .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
            .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64")) > config.maxPrecision)
            .map(Field::name)
            .collect(Collectors.toSet());
        State result;

        if (decimalFields.size() == 0) {
          result = NOOP;
        } else {
          log.trace("state() - processing schema '{}'", schema.name());
          SchemaBuilder builder = SchemaBuilder.struct()
              .name(inputSchema.name())
              .doc(inputSchema.doc())
              .version(inputSchema.version());
          if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
            builder.parameters(inputSchema.parameters());
          }

          for (Field field : inputSchema.fields()) {
            log.trace("state() - processing field '{}'", field.name());
            if (decimalFields.contains(field.name())) {
              Map<String, String> parameters = new LinkedHashMap<>();
              if (null != field.schema().parameters() && !field.schema().parameters().isEmpty()) {
                parameters.putAll(field.schema().parameters());
              }
              parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.maxPrecision));
              int scale = Integer.parseInt(parameters.get(Decimal.SCALE_FIELD));
              SchemaBuilder fieldBuilder = Decimal.builder(scale)
                  .parameters(parameters)
                  .doc(field.schema().doc())
                  .version(field.schema().version());
              if (field.schema().isOptional()) {
                fieldBuilder.optional();
              }
              Schema fieldSchema = fieldBuilder.build();
              builder.field(field.name(), fieldSchema);
            } else {
              log.trace("state() - copying field '{}' to new schema.", field.name());
              builder.field(field.name(), field.schema());
            }
          }

          Schema outputSchema = builder.build();
          result = new State(false, outputSchema, decimalFields);
        }


        return result;
      }
    });

  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    State state = state(inputSchema);
    SchemaAndValue result;

    if (state.noop) {
      result = new SchemaAndValue(inputSchema, input);
    } else {
      Struct struct = new Struct(state.outputSchema);
      for (Field field : inputSchema.fields()) {
        struct.put(field.name(), input.get(field.name()));
      }
      result = new SchemaAndValue(state.outputSchema, struct);
    }
    return result;
  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new SetMaximumPrecisionConfig(settings);
  }

  public static class Key<R extends ConnectRecord<R>> extends SetMaximumPrecision<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends SetMaximumPrecision<R> {
    public Value() {
      super(false);
    }
  }
}
