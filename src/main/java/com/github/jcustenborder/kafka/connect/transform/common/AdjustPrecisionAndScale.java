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
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Title("AdjustPrecisionAndScale")
@Description("This transformation is used to ensure that all decimal fields in a struct fall within" +
        "the desired range.  Can set a max precision and max scale, as well as require a positive scale.")
@DocumentationNote("The Confluent AvroConverter uses a default precision of 64 which can be too large " +
    "for some database systems.")
public class AdjustPrecisionAndScale<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(AdjustPrecisionAndScale.class);

  public AdjustPrecisionAndScale(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return AdjustPrecisionAndScaleConfig.config();
  }

  @Override
  public void close() {

  }

  AdjustPrecisionAndScaleConfig config;

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
  static final String CONNECT_DECIMAL_SCALE_PROP = "scale";

  State state(Schema inputSchema) {
    return this.schemaLookup.computeIfAbsent(inputSchema, new Function<Schema, State>() {
      @Override
      public State apply(Schema schema) {
        Set<String> precisionFields; // Fields that have either undefined precision or precision that exceeds provided value
        Set<String> scaleFields; // Fields that have either undefined scale or scale that exceeds provided value
        Set<String> negativeScaleFields; // Fields that have a negative scale

        // Depending on mode, get DECIMAL fields that:
        // * Have undefined precision (precision not set)
        // * Have undefined precision OR precision that exceeds the provided value
        if (config.precisionMode == AdjustPrecisionAndScaleConfig.PRECISION_MODE_UNDEFINED) {
          precisionFields = inputSchema.fields().stream()
              .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
              .filter(f -> ! f.schema().parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP))
              .map(Field::name)
              .collect(Collectors.toSet());
        }
        else if (config.precisionMode == AdjustPrecisionAndScaleConfig.PRECISION_MODE_MAX) {
          precisionFields = inputSchema.fields().stream()
              .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
              .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "2147483647")) > config.precision)
              .map(Field::name)
              .collect(Collectors.toSet());
        } else {
          precisionFields = Collections.emptySet();
        }

        // To detect undefined SCALE, look for the following two conditions:
        //    PRECISION is undefined AND
        //    SCALE is set to the 'provided' value of 127 (or other provided default)
        // Also, if in SCALE_MODE_MAX (which looks for either max or undefined), look for SCALE which exceeds provided value and add it to the Set
        if (config.scaleMode == AdjustPrecisionAndScaleConfig.SCALE_MODE_UNDEFINED || config.scaleMode == AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX) {
          scaleFields = inputSchema.fields().stream()
            .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
            .filter(f -> ! f.schema().parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP))
            .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_DECIMAL_SCALE_PROP, Integer.toString(config.scale))) == config.scale)
            .map(Field::name)
            .collect(Collectors.toSet());
          if (config.scaleMode == AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX) {
            scaleFields.addAll(
              inputSchema.fields().stream()
                .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
                .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_DECIMAL_SCALE_PROP, "2147483647")) > config.scale)
                .map(Field::name)
                .collect(Collectors.toSet())
            );
          }
        } else {
          scaleFields = Collections.emptySet();
        }

        // Only calculate negativeScaleFields if it's going to be used (non-none mode)
        if(config.scaleNegativeMode != AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_NONE) {
          negativeScaleFields = inputSchema.fields().stream()
            .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
            .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_DECIMAL_SCALE_PROP, "0")) < 0)
            .map(Field::name)
            .collect(Collectors.toSet());
        } else {
          negativeScaleFields = Collections.emptySet();
        }

        State result;

        // temp fields for a given iteration
        int scale;
        boolean setPrecision, setScalePositive, setScaleNegative;

        if (precisionFields.size() == 0 && scaleFields.size() == 0 && negativeScaleFields.size() == 0) {
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
            setPrecision = precisionFields.contains(field.name());
            setScalePositive = scaleFields.contains(field.name());
            setScaleNegative = negativeScaleFields.contains(field.name());

            log.trace("state() - processing field '{}'", field.name());
            if (setPrecision || setScalePositive || setScaleNegative) {
              Map<String, String> parameters = new LinkedHashMap<>();
              if (null != field.schema().parameters() && !field.schema().parameters().isEmpty()) {
                parameters.putAll(field.schema().parameters());
              }

              if (setPrecision) {
                // Doesn't matter if we're setting because it's undefined or value greater than provided
                parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.precision));
              }

              // For a given parameter, setScalePositive and setScaleNegative can't both be true
              // If initial scale is negative and negative scale mode is 'zero', set to 0
              // Otherwise, if any of the following conditions, set scale to provided scale value:
              // * Initial scale 'undefined' (precision is undefined, scale is 'default value')
              // * Initial scale greater than provided value AND scale mode is 'max'
              // * Initial scale negative and negative scale mode is 'value'
              // Otherwise leave scale alone
              if (setScaleNegative &&
                  config.scaleNegativeMode == AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_ZERO) {
                parameters.put(CONNECT_DECIMAL_SCALE_PROP, "0");
                scale = 0;
              } else if (
                  setScalePositive ||
                  (setScaleNegative &&
                      config.scaleNegativeMode == AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_VALUE)
              ) {
                parameters.put(CONNECT_DECIMAL_SCALE_PROP, Integer.toString(config.scale));
                scale = config.scale;
              } else {
                scale = Integer.parseInt(parameters.get(Decimal.SCALE_FIELD));
              }

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
          result = new State(false, outputSchema, precisionFields);
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
    this.config = new AdjustPrecisionAndScaleConfig(settings);
  }

  public static class Key<R extends ConnectRecord<R>> extends AdjustPrecisionAndScale<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends AdjustPrecisionAndScale<R> {
    public Value() {
      super(false);
    }
  }
}
