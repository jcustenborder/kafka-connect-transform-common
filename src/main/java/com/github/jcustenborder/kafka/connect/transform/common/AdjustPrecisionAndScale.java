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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.LinkedHashMap;

@Title("AdjustPrecisionAndScale")
@Description("This transformation is used to ensure that all decimal fields in a struct fall within" +
        "the desired range.  Can set a max precision and max scale, as well as require a positive scale.")
@DocumentationNote("The Confluent AvroConverter uses a default precision of 64 which can be too large " +
    "for some database systems.")
public abstract class AdjustPrecisionAndScale<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
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

  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
  static final String CONNECT_AVRO_DECIMAL_SCALE_PROP = "scale";

  // Streams/lambdas don't add a ton of value here; becomes both really hard to reason about and not really that clean
  // Also, for loops are faster
  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    Set<String> modifiedFields = new HashSet<>();

    Schema outputSchema;
    Struct outputStruct;

    SchemaBuilder builder = SchemaBuilder.struct()
        .name(inputSchema.name())
        .doc(inputSchema.doc())
        .version(inputSchema.version());
    if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
      builder.parameters(inputSchema.parameters());
    }

    // Iterate over all fields to generate new schemas
    // Only perform logic on 'org.apache.kafka.connect.data.Decimal' fields; otherwise, directly copy field schema to new schema
    for (Field field: inputSchema.fields()) {
      if (Decimal.LOGICAL_NAME.equals(field.schema().name())) {
        log.trace("Looking at {}", field.name());

        int scale, precision;
        // If we're coming from a Connector that doesn't have precision defined, default to 'undefined precision' (which is 0)
        precision = Integer.parseInt(inputSchema.field(field.name()).schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "0"));
        scale = Integer.parseInt(inputSchema.field(field.name()).schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_SCALE_PROP, "0"));

        boolean undefinedPrecision = precision == config.undefinedPrecisionValue;
        boolean exceededPrecision = precision > config.precision;

        // If precision is undefined, we assume scale is undefined as well
        boolean undefinedScale = undefinedPrecision || scale == config.undefinedScaleValue;
        boolean zeroScale = scale == 0;
        boolean exceededScale = scale > config.scale;
        boolean negativeScale = scale < 0;

        // If in undefined mode, set precision to provided value if precision is undefined
        // If in max mode, set precision to provided value if precision is undefined or exceeds provided value
        boolean setPrecision = (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_UNDEFINED) && undefinedPrecision) ||
            (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_MAX) && (undefinedPrecision || exceededPrecision));

        // Set scale to provided scale value if any of the following are true:
        // scale mode is 'undefined' and scale is undefined
        // scale mode is 'max' and scale is undefined or over value
        // scale zero mode is 'value' and scale is zero
        // scale negative mode is 'value' and scale is negative
        boolean setScaleValue = (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_UNDEFINED) && undefinedScale) ||
            (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX) && (undefinedScale || exceededScale)) ||
            (config.scaleZeroMode.equals(AdjustPrecisionAndScaleConfig.SCALE_ZERO_MODE_VALUE) && zeroScale) ||
            (config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_VALUE) && negativeScale);

        // Set scale to zero if any of the following are true:
        // scale negative mode is 'zero' and scale is negative
        boolean setScaleZero = (config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_ZERO) && negativeScale);

        // Do nothing to scale in these situations:
        // scale mode is none OR (scale is positive and does not exceed provided value)
        // AND scale negative mode is none
        // AND scale zero mode is none OR scale is nonzero

        Map<String, String> parameters = new LinkedHashMap<>();
        if (null != field.schema().parameters() && !field.schema().parameters().isEmpty()) {
          parameters.putAll(field.schema().parameters());
        }

        // Set precision to provided value or pull from struct schema
        if (setPrecision) {
          parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.precision));
        } else {
          parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(precision));
        }

        // Set scale to provided value, to zero, or pull from struct schema
        if (setScaleValue) {
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(config.scale));
          scale = config.scale;
        } else if (setScaleZero) {
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(0));
          scale = 0;
        } else {
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(scale));
        }

        if (setPrecision || setScaleValue || setScaleZero) {
          modifiedFields.add(field.name());
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
        // Not a Decimal
        log.trace("state() - copying field '{}' to new schema.", field.name());
        builder.field(field.name(), field.schema());
      }
    }

    outputSchema = builder.build();
    outputStruct = new Struct(outputSchema);

    // Hydrate Struct by iterating over fields again
    for (Field field: outputSchema.fields()) {
//      String fieldName = field.name();

      if (modifiedFields.contains(field.name())) {
        BigDecimal originalBigDecimal = (BigDecimal) inputStruct.get(field.name());
        if (null != originalBigDecimal) {
          int precision = Integer.parseInt(field.schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
          int scale = Integer.parseInt(field.schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));

          // RoundingMode _shouldn't_ matter here because the source data presumably has the same precision and scale;
          // it was just 'lost' (not picked up) by the Connector (prior to the SMT)
          // Precision of the BigDecimal will be total scale + total number of digits to left of decimal
          // For example: 12345.67890 with a scale of 5 will have precision of 10, regardless of desired precision,
          // but the schema will reflect both desired precision and scale
          // Order of scale vs. round doesn't seem to matter here
          MathContext mc = new MathContext(precision);
          BigDecimal newBigDecimal = originalBigDecimal.round(mc).setScale(scale, RoundingMode.FLOOR);
          outputStruct.put(field.name(), newBigDecimal);
        } else {
          outputStruct.put(field.name(), null);
        }
      } else {
        log.trace("state() - copying field '{}' to new struct.", field.name());
        outputStruct.put(field.name(), inputStruct.get(field.name()));
      }
    }

    return new SchemaAndValue(outputSchema, outputStruct);
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
