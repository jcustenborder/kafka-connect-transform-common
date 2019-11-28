package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.data.SchemaBuilders;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class ConversionHandlerTest {
  @TestFactory
  public Stream<DynamicTest> parse() {
    Multimap<Schema, Object> tests = LinkedListMultimap.create();
    tests.put(Schema.BOOLEAN_SCHEMA, true);
    tests.put(Schema.BOOLEAN_SCHEMA, false);
    tests.put(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE);
    tests.put(Schema.FLOAT32_SCHEMA, Float.MIN_VALUE);
    tests.put(Schema.FLOAT64_SCHEMA, Double.MIN_VALUE);
    tests.put(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE);
    tests.put(Schema.INT8_SCHEMA, Byte.MIN_VALUE);
    tests.put(Schema.INT8_SCHEMA, Byte.MAX_VALUE);
    tests.put(Schema.INT16_SCHEMA, Short.MIN_VALUE);
    tests.put(Schema.INT16_SCHEMA, Short.MAX_VALUE);
    tests.put(Schema.INT32_SCHEMA, Integer.MIN_VALUE);
    tests.put(Schema.INT32_SCHEMA, Integer.MAX_VALUE);
    tests.put(Schema.INT64_SCHEMA, Long.MIN_VALUE);
    tests.put(Schema.INT64_SCHEMA, Long.MAX_VALUE);
    tests.put(Timestamp.SCHEMA, new Date(0));

    for (int i = 0; i < 20; i++) {
      Schema schema = Decimal.schema(i);
      tests.put(schema, BigDecimal.valueOf(Long.MAX_VALUE, i));
      tests.put(schema, BigDecimal.valueOf(Long.MIN_VALUE, i));
    }


    return tests.entries().stream()
        .map(e -> dynamicTest(String.format(
            "%s - %s",
            ConversionHandler.SchemaKey.of(e.getKey()),
            e.getValue()
        ), () -> {
          final Schema schema = e.getKey();
          final Object expected = e.getValue();
          final String headerName = "input";
          final String fieldName = "output";
          Headers inputHeaders = new ConnectHeaders();
          inputHeaders.add(headerName, expected, schema);
          Schema inputSchema = SchemaBuilder.struct()
              .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
              .build();
          Struct inputStruct = new Struct(inputSchema)
              .put("firstName", "Test");
          SinkRecord inputRecord = new SinkRecord(
              "testing",
              1,
              null,
              null,
              inputStruct.schema(),
              inputStruct,
              12345L,
              123412351L,
              TimestampType.NO_TIMESTAMP_TYPE,
              inputHeaders
          );
          Schema outputSchema = SchemaBuilders.of(inputSchema)
              .field(fieldName, schema)
              .build();
          Struct outputStruct = new Struct(outputSchema);
          ConversionHandler handler = ConversionHandler.of(schema, headerName, fieldName);
          assertNotNull(handler, "handler cannot be null.");
          handler.convert(inputRecord, outputStruct);
          Object actual = outputStruct.get(fieldName);
          assertEquals(expected, actual);
        }));


  }
}
