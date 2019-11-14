package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.base.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SchemaNameToTopicTest {
  Transformation<SinkRecord> transformation  = new SchemaNameToTopic.Value<>();
  SinkRecord exampleRecord(Schema schema) {
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      struct.put(field, Strings.repeat("x", 50));
    }
    return new SinkRecord(
        "test",
        0,
        null,
        null,
        schema,
        struct,
        1234L
    );

  }

  Schema exampleSchema(List<String> fieldNames, final int version) {
    SchemaBuilder builder = SchemaBuilder.struct()
        .name(this.getClass().getName());
    for (String fieldName : fieldNames) {
      builder.field(fieldName, Schema.STRING_SCHEMA);
    }
    builder.version(version);
    return builder.build();
  }

  @Test
  public void apply() {
    Schema schema = SchemaBuilder.struct()
        .name("com.foo.bar.whatever.ASDF")
        .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    SinkRecord input = exampleRecord(schema);
    SinkRecord actual = this.transformation.apply(input);
    assertNotNull(actual);
    assertEquals(schema.name(), actual.topic());


  }


}
