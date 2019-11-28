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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

abstract class ConversionHandler {
  final Schema headerSchema;
  final String header;
  final String field;

  protected ConversionHandler(Schema headerSchema, String header, String field) {
    this.headerSchema = headerSchema;
    this.header = header;
    this.field = field;
  }

  abstract Object convert(Header header);

  public void convert(ConnectRecord record, Struct struct) {
    final Header header = record.headers().lastWithName(this.header);
    Object fieldValue;
    if (null != header) {
      fieldValue = convert(header);
    } else {
      fieldValue = null;
    }

    struct.put(this.field, fieldValue);
  }

  static class StringConversionHandler extends ConversionHandler {
    public StringConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToString(header.schema(), header.value());
    }
  }

  static class BooleanConversionHandler extends ConversionHandler {
    public BooleanConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToBoolean(header.schema(), header.value());
    }
  }

  static class Float32ConversionHandler extends ConversionHandler {
    public Float32ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToFloat(header.schema(), header.value());
    }
  }

  static class Float64ConversionHandler extends ConversionHandler {
    public Float64ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToDouble(header.schema(), header.value());
    }
  }

  static class Int8ConversionHandler extends ConversionHandler {
    public Int8ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToByte(header.schema(), header.value());
    }
  }

  static class Int16ConversionHandler extends ConversionHandler {
    public Int16ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToShort(header.schema(), header.value());
    }
  }

  static class Int32ConversionHandler extends ConversionHandler {
    public Int32ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToInteger(header.schema(), header.value());
    }
  }

  static class Int64ConversionHandler extends ConversionHandler {
    public Int64ConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToLong(header.schema(), header.value());
    }
  }

  static class DecimalConversionHandler extends ConversionHandler {
    private final int scale;

    public DecimalConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
      String scaleText = null != headerSchema.parameters() ? headerSchema.parameters().get(Decimal.SCALE_FIELD) : null;
      Preconditions.checkNotNull(scaleText, "schema parameters must contain a '%s' parameter.", Decimal.SCALE_FIELD);
      scale = Integer.parseInt(scaleText);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToDecimal(header.schema(), header.value(), scale);
    }
  }

  static class TimestampConversionHandler extends ConversionHandler {

    public TimestampConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToTimestamp(header.schema(), header.value());
    }
  }

  static class TimeConversionHandler extends ConversionHandler {

    public TimeConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToTime(header.schema(), header.value());
    }
  }

  static class DateConversionHandler extends ConversionHandler {

    public DateConversionHandler(Schema headerSchema, String header, String field) {
      super(headerSchema, header, field);
    }

    @Override
    Object convert(Header header) {
      return Values.convertToDate(header.schema(), header.value());
    }
  }


  static class SchemaKey implements Comparable<SchemaKey> {
    final String name;
    final Schema.Type type;


    SchemaKey(String name, Schema.Type type) {
      this.name = name;
      this.type = type;
    }

    public static SchemaKey of(Schema schema) {
      return new SchemaKey(schema.name(), schema.type());
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.type, this.name);
    }

    @Override
    public int compareTo(SchemaKey that) {
      return ComparisonChain.start()
          .compare(this.type, that.type)
          .compare(this.name, that.name, Ordering.natural().nullsLast())
          .result();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SchemaKey) {
        return 0 == compareTo((SchemaKey) obj);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .omitNullValues()
          .add("type", this.type)
          .add("name", this.name)
          .toString();
    }
  }

  interface ConversionHandlerFactory {
    ConversionHandler create(Schema schema, String header, String field);
  }

  static final Map<SchemaKey, ConversionHandlerFactory> CONVERSION_HANDLER_FACTORIES;

  static {
    Map<SchemaKey, ConversionHandlerFactory> handlerFactories = new HashMap<>();
    handlerFactories.put(SchemaKey.of(Schema.STRING_SCHEMA), StringConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.BOOLEAN_SCHEMA), BooleanConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.FLOAT32_SCHEMA), Float32ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.FLOAT64_SCHEMA), Float64ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.INT8_SCHEMA), Int8ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.INT16_SCHEMA), Int16ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.INT32_SCHEMA), Int32ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Schema.INT64_SCHEMA), Int64ConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Decimal.schema(1)), DecimalConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Timestamp.SCHEMA), TimestampConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Time.SCHEMA), TimeConversionHandler::new);
    handlerFactories.put(SchemaKey.of(Date.SCHEMA), DateConversionHandler::new);


    CONVERSION_HANDLER_FACTORIES = ImmutableMap.copyOf(handlerFactories);
  }


  public static ConversionHandler of(Schema headerSchema, String header, String field) {
    SchemaKey key = SchemaKey.of(headerSchema);
    ConversionHandlerFactory factory = CONVERSION_HANDLER_FACTORIES.get(key);
    if (null == factory) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", key)
      );
    }
    return factory.create(headerSchema, header, field);
  }
}
