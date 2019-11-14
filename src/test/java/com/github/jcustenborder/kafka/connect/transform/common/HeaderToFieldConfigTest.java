package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class HeaderToFieldConfigTest {
  private static final Logger log = LoggerFactory.getLogger(HeaderToFieldConfigTest.class);

  void addTest(Schema schema, Map<String, HeaderToFieldConfig.HeaderToFieldMapping> tests) {
    String schemaText = HeaderToFieldConfig.HeaderToFieldMapping.toString(schema);
    StringBuilder builder = new StringBuilder();
    builder.append("foo:");
    builder.append(schemaText);
    tests.put(
        builder.toString(),
        new HeaderToFieldConfig.HeaderToFieldMapping(
            "foo",
            schema,
            "foo"
        )
    );
    builder.append(":bar");
    tests.put(
        builder.toString(),
        new HeaderToFieldConfig.HeaderToFieldMapping(
            "foo",
            schema,
            "bar"
        )
    );
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    Set<Schema.Type> skip = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT);
    Map<String, HeaderToFieldConfig.HeaderToFieldMapping> tests = new LinkedHashMap<>();

    addTest(Timestamp.builder().optional().build(), tests);
    addTest(Date.builder().optional().build(), tests);
    addTest(Time.builder().optional().build(), tests);
    IntStream.range(0, 50)
        .mapToObj(value -> Decimal.builder(value).optional().build())
        .forEach(schema -> addTest(schema, tests));


    Arrays.stream(Schema.Type.values())
        .filter(type -> !skip.contains(type))
        .forEach(type -> {
          Schema schema = SchemaBuilder.type(type)
              .optional()
              .build();
          addTest(schema, tests);
        });
    log.info("{}", Joiner.on('\n').join(tests.keySet()));


    return tests.entrySet().stream()
        .map(e -> dynamicTest(e.getKey(), () -> {
          final HeaderToFieldConfig.HeaderToFieldMapping expected = e.getValue();
          final HeaderToFieldConfig.HeaderToFieldMapping actual = HeaderToFieldConfig.HeaderToFieldMapping.parse(e.getKey());
          assertNotNull(actual, "actual should not be null.");
          assertEquals(expected, actual, "mappings do not match");
        }));
  }

}
