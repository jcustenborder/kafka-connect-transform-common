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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

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

  static final State NOOP = new State(true, null, null, null);

  static class State {
    public final boolean noop;
    public final Schema outputSchema;
    public final Struct outputStruct;
    public final Set<String> decimalFields;

    State(boolean noop, Schema outputSchema, Struct outputStruct, Set<String> decimalFields) {
      this.noop = noop;
      this.outputSchema = outputSchema;
      this.outputStruct = outputStruct;
      this.decimalFields = decimalFields;
    }
  }

  Map<Schema, State> schemaLookup = new HashMap<>();

  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
  static final String CONNECT_AVRO_DECIMAL_SCALE_PROP = "scale";

  //  State process(Schema inputSchema, Struct inputStruct) {
  //    State result;
  //    Schema outputSchema;
  //    Struct outputStruct;
  //
  //    log.info("We are here now");
  //
  //    // These sets of fields that need to be modified
  //    Set<String> precisionFields; // Fields that have either undefined precision or precision that exceeds provided value
  //    Set<String> scaleFields; // Fields that have either undefined scale or scale that exceeds provided value
  //    Set<String> negativeScaleFields; // Fields that have a negative scale
  //
  //    // Determine which fields have precisions that need to be modified
  //    // TODO: Switch this to use inputStruct AND inputSchema
  //    // * Have undefined precision (precision not set)
  //    // * Have undefined precision OR precision that exceeds the provided value
  //    if (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_UNDEFINED)) {
  //      log.info("HERE: Looking for undefined precision");
  //      precisionFields = inputSchema.fields().stream()
  //          .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
  //          .filter(f -> !f.schema().parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP))
  //          .map(Field::name)
  //          .collect(Collectors.toSet());
  //    } else if (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_MAX)) {
  //      log.info("HERE: Looking for high precision");
  //      precisionFields = inputSchema.fields().stream()
  //          .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
  //          .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "2147483647")) > config.precision)
  //          .map(Field::name)
  //          .collect(Collectors.toSet());
  //    } else {
  //      log.info("HERE: Not looking at precision");
  //      precisionFields = Collections.emptySet();
  //    }
  //
  //    // Determine which fields have scales that are undefined or exceed max
  //    // TODO: Switch this to use inputStruct rather AND inputSchema
  //    //    PRECISION is undefined AND
  //    //    SCALE is set to the 'provided' value of 127 (or other provided default)
  //    // Also, if in SCALE_MODE_MAX (which looks for either max or undefined), look for SCALE which exceeds provided value and add it to the Set
  //    if (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_UNDEFINED) || config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX)) {
  //      scaleFields = inputSchema.fields().stream()
  //          .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
  //          .filter(f -> !f.schema().parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP))
  //          .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(config.scale))) == config.scale)
  //          .map(Field::name)
  //          .collect(Collectors.toSet());
  //      if (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX)) {
  //        scaleFields.addAll(
  //            inputSchema.fields().stream()
  //                .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
  //                .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_SCALE_PROP, "2147483647")) > config.scale)
  //                .map(Field::name)
  //                .collect(Collectors.toSet())
  //        );
  //      }
  //    } else {
  //      scaleFields = Collections.emptySet();
  //    }
  //
  //    // Only calculate negativeScaleFields if it's going to be used (non-none mode)
  //    if (!config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_NONE)) {
  //      negativeScaleFields = inputSchema.fields().stream()
  //          .filter(f -> Decimal.LOGICAL_NAME.equals(f.schema().name()))
  //          .filter(f -> Integer.parseInt(f.schema().parameters().getOrDefault(CONNECT_AVRO_DECIMAL_SCALE_PROP, "0")) < 0)
  //          .map(Field::name)
  //          .collect(Collectors.toSet());
  //    } else {
  //      negativeScaleFields = Collections.emptySet();
  //    }
  //
  //    // temp fields for a given iteration
  //    int scale;
  //    boolean setPrecision, setScalePositive, setScaleNegative;
  //    log.info("precisionFields: {} scaleFields: {} negativeScaleFields: {}",
  //        precisionFields.size(),
  //        scaleFields.size(),
  //        negativeScaleFields.size());
  //
  //    if (precisionFields.size() == 0 && scaleFields.size() == 0 && negativeScaleFields.size() == 0) {
  //      result = NOOP;
  //    } else {
  //      log.info("state() - processing schema '{}'", inputSchema.name());
  //      SchemaBuilder builder = SchemaBuilder.struct()
  //          .name(inputSchema.name())
  //          .doc(inputSchema.doc())
  //          .version(inputSchema.version());
  //      if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
  //        builder.parameters(inputSchema.parameters());
  //      }
  //
  //      // We have to iterate over this twice; once to generate the new schema; then again to generate the new output Struct
  //      for (Field field : inputSchema.fields()) {
  //        setPrecision = precisionFields.contains(field.name());
  //        setScalePositive = scaleFields.contains(field.name());
  //        setScaleNegative = negativeScaleFields.contains(field.name());
  //
  //      //        Set<String> p = field.schema().parameters().keySet();
  //      //        for (String param: p) {
  //      //          log.info(param);
  //      //          log.info(field.schema().parameters().get(param));
  //      //        }
  //
  //        log.info("state() - processing field '{}' with settings " +
  //                "setPrecision [{}] setScalePositive [{}] setScaleNegative [{}]",
  //            field.name(),
  //            setPrecision,
  //            setScalePositive,
  //            setScaleNegative);
  //        if (setPrecision || setScalePositive || setScaleNegative) {
  //          Map<String, String> parameters = new LinkedHashMap<>();
  //          if (null != field.schema().parameters() && !field.schema().parameters().isEmpty()) {
  //            parameters.putAll(field.schema().parameters());
  //          }
  //
  //          if (setPrecision) {
  //            // Doesn't matter if we're setting because it's undefined or value greater than provided
  //            parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.precision));
  //          }
  //
  //          // For a given parameter, setScalePositive and setScaleNegative can't both be true
  //          // If initial scale is negative and negative scale mode is 'zero', set to 0
  //          // Otherwise, if any of the following conditions, set scale to provided scale value:
  //          // * Initial scale 'undefined' (precision is undefined, scale is 'default value')
  //          // * Initial scale greater than provided value AND scale mode is 'max'
  //          // * Initial scale negative and negative scale mode is 'value'
  //          // Otherwise leave scale alone
  //          if (setScaleNegative &&
  //              config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_ZERO)) {
  //            parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, "0");
  //            scale = 0;
  //          } else if (
  //              setScalePositive ||
  //                  (setScaleNegative &&
  //                      config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_VALUE))
  //          ) {
  //            parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(config.scale));
  //            scale = config.scale;
  //          } else {
  //            scale = Integer.parseInt(parameters.get(Decimal.SCALE_FIELD));
  //          }
  //
  //          SchemaBuilder fieldBuilder = Decimal.builder(scale)
  //              .parameters(parameters)
  //              .doc(field.schema().doc())
  //              .version(field.schema().version());
  //
  //          if (field.schema().isOptional()) {
  //            fieldBuilder.optional();
  //          }
  //          Schema fieldSchema = fieldBuilder.build();
  //          builder.field(field.name(), fieldSchema);
  //        } else {
  //          log.info("state() - copying field '{}' to new schema.", field.name());
  //          builder.field(field.name(), field.schema());
  //        }
  //      }
  //
  //      outputSchema = builder.build();
  //
  //      outputStruct = new Struct(outputSchema);
  //
  //      BigDecimal decimalValue;
  //
  //      // Second iteration
  //      for (Field field: outputSchema.fields()) {
  //        setPrecision = precisionFields.contains(field.name());
  //        setScalePositive = scaleFields.contains(field.name());
  //        setScaleNegative = negativeScaleFields.contains(field.name());
  //
  //        log.info("processing field '{}' for output struct with settings " +
  //                "setPrecision [{}] setScalePositive [{}] setScaleNegative [{}]",
  //            field.name(),
  //            setPrecision,
  //            setScalePositive,
  //            setScaleNegative);
  //
  //        if (setPrecision || setScalePositive || setScaleNegative) {
  //          decimalValue = (BigDecimal) inputStruct.get(field.name());
  //
  //          if (setPrecision) {
  //            log.info("Original precision is {}", decimalValue.precision());
  //          }
  //
  //          // For a given parameter, setScalePositive and setScaleNegative can't both be true
  //          // If initial scale is negative and negative scale mode is 'zero', set to 0
  //          // Otherwise, if any of the following conditions, set scale to provided scale value:
  //          // * Initial scale 'undefined' (precision is undefined, scale is 'default value')
  //          // * Initial scale greater than provided value AND scale mode is 'max'
  //          // * Initial scale negative and negative scale mode is 'value'
  //          // Otherwise leave scale alone
  //          if (setScaleNegative &&
  //              config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_ZERO)) {
  //            log.info("Original scale is {}, set to 0", decimalValue.scale());
  //            decimalValue = decimalValue.setScale(0);
  //          } else if (
  //              setScalePositive ||
  //                  (setScaleNegative &&
  //                      config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_VALUE))
  //          ) {
  //            log.info("Original scale is {}, set to provided value", decimalValue.scale());
  //            decimalValue = decimalValue.setScale(config.scale);
  //          } else {
  //            log.info("Original scale is {}, leaving alone", decimalValue.scale());
  //          }
  //
  //          outputStruct.put(field.name(), decimalValue);
  //        } else {
  //          log.info("state() - copying field '{}' to new struct.", field.name());
  //          outputStruct.put(field.name(), inputStruct.get(field.name()));
  //        }
  //      }
  //
  //      // Validator
  //      for (Field field: outputSchema.fields()) {
  //        log.info(field.name());
  //        log.info(field.schema().name());
  //        Map<String, String> map = inputSchema.field(field.name()).schema().parameters();
  //        for (String key: map.keySet()) {
  //          log.info("Parameter {} value {}", key, map.get(key));
  //        }
  //        log.info("Input Schema Precision: {}", inputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
  //        log.info("Input BD Precision: {}", ((BigDecimal) inputStruct.get(field.name())).precision());
  //        log.info("Input Schema Scale: {}", inputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));
  //        log.info("Input BD Scale: {}", ((BigDecimal) inputStruct.get(field.name())).scale());
  //        log.info("Output Schema Precision: {}", outputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
  //        log.info("Output BD Precision: {}", ((BigDecimal) outputStruct.get(field.name())).precision());
  //        log.info("Output Schema Scale: {}", outputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));
  //        log.info("Output BD Scale: {}", ((BigDecimal) outputStruct.get(field.name())).scale());
  //      }
  //
  //      result = new State(false, outputSchema, outputStruct, precisionFields);
  //    }
  //    return result;
  //  }

  //  @Override
  //  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
  //    log.info("I AM HERE");
  //    for (Field f : inputSchema.fields()) {
  //      log.info(input.get(f).toString());
  //    }
  //    State state = process(inputSchema, input);
  //    SchemaAndValue result;
  //
  //    if (state.noop) {
  //      result = new SchemaAndValue(inputSchema, input);
  //    } else {
  //      result = new SchemaAndValue(state.outputSchema, state.outputStruct);
  //    }
  //    return result;
  //  }

  // Streams/lambdas don't add a ton of value here, cause we end up with like 6 streams
  // Also for loops are faster
  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    log.info("I AM HERE");

//    Set<String> precisionFields = new HashSet<>(); // Fields that have either undefined precision or precision that exceeds provided value
//    Set<String> scalePositiveFields = new HashSet<>(); // Fields that have either undefined scale or scale that exceeds provided value
//    Set<String> negativeScaleFields = new HashSet<>(); // Fields that have a negative scale
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
    // Only perform logic on 'org.apache.kafka.connect.data.Decimal' fields
    for (Field field: inputSchema.fields()) {
      if (Decimal.LOGICAL_NAME.equals(field.schema().name())) {
        String fieldName = field.name();
        BigDecimal originalBigDecimal = (BigDecimal) inputStruct.get(fieldName);
        log.info("Looking at {}", fieldName);

        int scale = originalBigDecimal.scale();
        int precision = originalBigDecimal.precision();

        // Need to look at input Value (inputValue) for precision
        // Scale can come either from input schema or inputValue
        boolean undefinedPrecision = precision == config.undefinedPrecisionValue;
        boolean exceededPrecision = precision > config.precision;
        boolean undefinedScale = undefinedPrecision && scale == config.undefinedScaleValue;
        boolean exceededScale = scale > config.scale;
        boolean negativeScale = scale < 0;

        // If in undefined mode, set precision to provided value if precision is undefined
        // If in max mode, set precision to provided value if precision is undefined or exceeds provided value
        boolean setPrecision = (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_UNDEFINED) && undefinedPrecision) ||
            (config.precisionMode.equals(AdjustPrecisionAndScaleConfig.PRECISION_MODE_MAX) && (undefinedPrecision || exceededPrecision));

        boolean setScalePositive = (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_UNDEFINED) && undefinedScale) ||
            (config.scaleMode.equals(AdjustPrecisionAndScaleConfig.SCALE_MODE_MAX) && (undefinedScale || exceededScale));

        boolean setScaleNegative = (negativeScale &&
            (config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_VALUE) ||
                config.scaleNegativeMode.equals(AdjustPrecisionAndScaleConfig.SCALE_NEGATIVE_MODE_ZERO)));

        Map<String, String> parameters = new LinkedHashMap<>();
        if (null != field.schema().parameters() && !field.schema().parameters().isEmpty()) {
          parameters.putAll(field.schema().parameters());
        }

        // Pull schema precision either from override or BigDecimal value
        if (setPrecision) {
//          precisionFields.add(fieldName);
          parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.precision));
        } else {
          parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(precision));
        }

        // Pull schema scale either from override, zero, or BigDecimal value
        // setScalePositive and setScaleNegative shouldn't both be true; if they are, positive has precedence
        if (setScalePositive) {
//          scalePositiveFields.add(fieldName);
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(config.scale));
          scale = config.scale;
        } else if (setScaleNegative) {
//          negativeScaleFields.add(fieldName);
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(0));
          scale = 0;
        } else {
          parameters.put(CONNECT_AVRO_DECIMAL_SCALE_PROP, Integer.toString(scale));
        }

        if (setPrecision || setScalePositive || setScaleNegative) {
          modifiedFields.add(fieldName);
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
        log.info("state() - copying field '{}' to new schema.", field.name());
        builder.field(field.name(), field.schema());
      }
    }

    outputSchema = builder.build();
    outputStruct = new Struct(outputSchema);

    // Now hydrate Struct by iterating over fields again
    for (Field field: outputSchema.fields()) {
      String fieldName = field.name();

      if (modifiedFields.contains(fieldName)) {
        BigDecimal originalBigDecimal = (BigDecimal) inputStruct.get(fieldName);
        int precision = Integer.parseInt(field.schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
        int scale = Integer.parseInt(field.schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));

        MathContext mc = new MathContext(precision);
        // RoundingMode _shouldn't_ matter here because the source data presumably has the same precision and scale;
        // it was just 'lost' by the Connector (prior to the SMT)
        BigDecimal newBigDecimal = originalBigDecimal.round(mc).setScale(scale, RoundingMode.DOWN);
        outputStruct.put(fieldName, newBigDecimal);
      } else {
        log.info("state() - copying field '{}' to new struct.", field.name());
        outputStruct.put(fieldName, inputStruct.get(field.name()));
      }
    }

    // Validation
    //    for (Field field: outputSchema.fields()) {
    //      log.info(field.name());
    //      log.info(field.schema().name());
    //      log.info("Input value: {}", ((BigDecimal) inputStruct.get(field.name())).toString());
    //      log.info("Input plain value: {}", ((BigDecimal) inputStruct.get(field.name())).toPlainString());
    //      log.info("Output value: {}", ((BigDecimal) outputStruct.get(field.name())).toPlainString());
    //      log.info("Output plain value: {}", ((BigDecimal) outputStruct.get(field.name())).toPlainString());
    //      log.info("Input Schema Precision: {}", inputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
    //      log.info("Input BD Precision: {}", ((BigDecimal) inputStruct.get(field.name())).precision());
    //      log.info("Input Schema Scale: {}", inputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));
    //      log.info("Input BD Scale: {}", ((BigDecimal) inputStruct.get(field.name())).scale());
    //      log.info("Output Schema Precision: {}", outputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP));
    //      log.info("Output BD Precision: {}", ((BigDecimal) outputStruct.get(field.name())).precision());
    //      log.info("Output Schema Scale: {}", outputSchema.field(field.name()).schema().parameters().get(CONNECT_AVRO_DECIMAL_SCALE_PROP));
    //      log.info("Output BD Scale: {}", ((BigDecimal) outputStruct.get(field.name())).scale());
    //      log.info("Parameters");
    //      Map<String, String> map = inputSchema.field(field.name()).schema().parameters();
    //      for (String key: map.keySet()) {
    //        log.info("Input parameter {} value {}", key, map.get(key));
    //      }
    //      map = outputSchema.field(field.name()).schema().parameters();
    //      for (String key: map.keySet()) {
    //        log.info("Outpu parameter {} value {}", key, map.get(key));
    //      }
    //    }

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
