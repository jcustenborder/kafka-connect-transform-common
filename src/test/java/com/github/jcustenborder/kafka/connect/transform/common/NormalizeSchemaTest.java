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

public class NormalizeSchemaTest {

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

  Transformation<SinkRecord> transformation = new NormalizeSchema.Value<>();

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
    List<List<String>> schemaFields = Arrays.asList(
        Arrays.asList("first_name"),
        Arrays.asList("first_name", "last_name"),
        Arrays.asList("first_name", "last_name", "email_address")
    );
    int version = 0;

    Map<Integer, Schema> schemaVersions = new LinkedHashMap<>();
    for (List<String> fieldNames : schemaFields) {
      schemaVersions.put(version, exampleSchema(fieldNames, version));
      version++;
    }
    Integer latestVersion = schemaVersions.keySet().stream()
        .max(Integer::compareTo)
        .get();
    Schema latestSchema = schemaVersions.get(latestVersion);
    SinkRecord latestRecord = exampleRecord(latestSchema);
    SinkRecord output = this.transformation.apply(latestRecord);
    assertNotNull(output);
    assertEquals(latestVersion, output.valueSchema().version());

    for (int i = 0; i < 50; i++) {
      int schemaVersion = i % schemaVersions.size();
      Schema schema = schemaVersions.get(schemaVersion);
      SinkRecord input = exampleRecord(schema);
      output = this.transformation.apply(input);
      assertNotNull(output);
      assertEquals(latestVersion, output.valueSchema().version());
    }
    schemaVersions.put(version, exampleSchema(Arrays.asList("first_name", "last_name", "email_address", "state"), version));
    latestVersion = schemaVersions.keySet().stream()
        .max(Integer::compareTo)
        .get();
    latestSchema = schemaVersions.get(latestVersion);
    latestRecord = exampleRecord(latestSchema);
    output = this.transformation.apply(latestRecord);

    for (int i = 0; i < 50; i++) {
      int schemaVersion = i % schemaVersions.size();
      Schema schema = schemaVersions.get(schemaVersion);
      SinkRecord input = exampleRecord(schema);
      output = this.transformation.apply(input);
      assertNotNull(output);
      assertEquals(latestVersion, output.valueSchema().version());
    }


  }


}
