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

import static org.junit.jupiter.api.Assertions.*;

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

  @Test
  public void applyWithNestedFieldCompatibility() {
    // Create version 1 schema with nested field containing one sub-field
    Schema nestedSchemaV1 = SchemaBuilder.struct()
        .name("NestedField")
        .field("subfield1", Schema.STRING_SCHEMA)
        .version(1)
        .build();
    
    Schema schemaV1 = SchemaBuilder.struct()
        .name("TestRecord")
        .field("id", Schema.STRING_SCHEMA)
        .field("nested", nestedSchemaV1)
        .version(1)
        .build();

    // Create version 2 schema with nested field containing an additional nullable sub-field
    Schema nestedSchemaV2 = SchemaBuilder.struct()
        .name("NestedField")
        .field("subfield1", Schema.STRING_SCHEMA)
        .field("subfield2", Schema.OPTIONAL_STRING_SCHEMA)  // Added nullable field
        .version(2)
        .build();
    
    Schema schemaV2 = SchemaBuilder.struct()
        .name("TestRecord")
        .field("id", Schema.STRING_SCHEMA)
        .field("nested", nestedSchemaV2)
        .version(2)
        .build();

    // Create a record with version 1 schema
    Struct nestedStructV1 = new Struct(nestedSchemaV1);
    nestedStructV1.put("subfield1", "value1");
    
    Struct structV1 = new Struct(schemaV1);
    structV1.put("id", "test-id");
    structV1.put("nested", nestedStructV1);
    
    SinkRecord recordV1 = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        schemaV1,
        structV1,
        1234L
    );

    // Create a record with version 2 schema to establish it as the latest version
    Struct nestedStructV2 = new Struct(nestedSchemaV2);
    nestedStructV2.put("subfield1", "value1_v2");
    nestedStructV2.put("subfield2", "value2_v2");
    
    Struct structV2 = new Struct(schemaV2);
    structV2.put("id", "test-id-v2");
    structV2.put("nested", nestedStructV2);
    
    SinkRecord recordV2 = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        schemaV2,
        structV2,
        1235L
    );

    // Apply transformation to establish version 2 as the latest
    SinkRecord outputV2 = this.transformation.apply(recordV2);
    assertNotNull(outputV2);
    assertEquals(2, outputV2.valueSchema().version());

    // Now apply transformation to version 1 record - it should be normalized to version 2
    SinkRecord outputV1 = this.transformation.apply(recordV1);
    
    // Verify the record was normalized to version 2 schema
    assertNotNull(outputV1);
    assertEquals(2, outputV1.valueSchema().version());
    assertEquals("TestRecord", outputV1.valueSchema().name());
    
    // Verify the output struct has the correct data
    Struct outputStruct = (Struct) outputV1.value();
    assertEquals("test-id", outputStruct.getString("id"));
    
    // Verify the nested structure was correctly normalized
    Struct outputNestedStruct = outputStruct.getStruct("nested");
    assertNotNull(outputNestedStruct);
    assertEquals("NestedField", outputNestedStruct.schema().name());
    assertEquals(2, outputNestedStruct.schema().version());
    
    // Verify original field value is preserved
    assertEquals("value1", outputNestedStruct.getString("subfield1"));
    
    // Verify new nullable field is null (backward compatible)
    assertEquals(null, outputNestedStruct.getString("subfield2"));
    
    // Verify the schema structure has both fields
    assertEquals(2, outputNestedStruct.schema().fields().size());
    assertNotNull(outputNestedStruct.schema().field("subfield1"));
    assertNotNull(outputNestedStruct.schema().field("subfield2"));
    
    // Verify the new field is optional (nullable)
    Field subfield2 = outputNestedStruct.schema().field("subfield2");
    assertEquals(true, subfield2.schema().isOptional());
  }

  @Test
  public void applyWithArrayFieldCompatibility() {
    // Create version 1 schema with nested array containing one struct item
    Schema nestedStructSchemaV1 = SchemaBuilder.struct()
        .name("NestedField")
        .field("subfield1", Schema.STRING_SCHEMA)
        .version(1)
        .build();

    Schema nestedArraySchemaV1 = SchemaBuilder.array(nestedStructSchemaV1)
        .name("NestedArray")
        .version(1)
        .build();

    Schema schemaV1 = SchemaBuilder.struct()
        .name("TestRecord")
        .field("id", Schema.STRING_SCHEMA)
        .field("nested", nestedArraySchemaV1)
        .version(1)
        .build();

    // Create version 2 schema with nested array containing one struct item that has an additional nullable sub-field
    Schema nestedStructSchemaV2 = SchemaBuilder.struct()
        .name("NestedField")
        .field("subfield1", Schema.STRING_SCHEMA)
        .field("subfield2", Schema.OPTIONAL_STRING_SCHEMA)  // Added nullable field
        .version(2)
        .build();

    Schema nestedArraySchemaV2 = SchemaBuilder.array(nestedStructSchemaV2)
        .name("NestedArray")
        .version(2)
        .build();

    Schema schemaV2 = SchemaBuilder.struct()
        .name("TestRecord")
        .field("id", Schema.STRING_SCHEMA)
        .field("nested", nestedArraySchemaV2)
        .version(2)
        .build();

    // Create a record with version 1 schema
    Struct deeplyNestedStructV1 = new Struct(nestedStructSchemaV1);
    deeplyNestedStructV1.put("subfield1", "value1");

    List<Struct> nestedStructV1 = Arrays.asList(deeplyNestedStructV1);

    Struct structV1 = new Struct(schemaV1);
    structV1.put("id", "test-id");
    structV1.put("nested", nestedStructV1);

    SinkRecord recordV1 = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        schemaV1,
        structV1,
        1234L
    );

    // Create a record with version 2 schema to establish it as the latest version
    Struct deeplyNestedStructV2 = new Struct(nestedStructSchemaV2);
    deeplyNestedStructV2.put("subfield1", "value1_v2");
    deeplyNestedStructV2.put("subfield2", "value2_v2");

    List<Struct> nestedStructV2 = Arrays.asList(deeplyNestedStructV2);

    Struct structV2 = new Struct(schemaV2);
    structV2.put("id", "test-id-v2");
    structV2.put("nested", nestedStructV2);

    SinkRecord recordV2 = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        schemaV2,
        structV2,
        1235L
    );

    // Apply transformation to establish version 2 as the latest
    SinkRecord outputV2 = this.transformation.apply(recordV2);
    assertNotNull(outputV2);
    assertEquals(2, outputV2.valueSchema().version());

    // Now apply transformation to version 1 record - it should be normalized to version 2
    SinkRecord outputV1 = this.transformation.apply(recordV1);

    // Verify the record was normalized to version 2 schema
    assertNotNull(outputV1);
    assertEquals(2, outputV1.valueSchema().version());
    assertEquals("TestRecord", outputV1.valueSchema().name());

    // Verify the output struct has the correct data
    Struct outputStruct = (Struct) outputV1.value();
    assertNotNull(outputStruct);
    assertEquals("test-id", outputStruct.getString("id"));

    // Verify the array is not empty in the nested field
    List<Struct> outputNestedArray = outputStruct.getArray("nested");
    assertEquals(1, outputNestedArray.size());

    // Verify the struct in array was correctly normalized
    Struct outputArrayStruct = outputNestedArray.get(0);
    assertEquals("NestedField", outputArrayStruct.schema().name());
    assertEquals(2, outputArrayStruct.schema().version());

    // Verify original field value is preserved
    assertEquals("value1", outputArrayStruct.getString("subfield1"));

    // Verify new nullable field is null (backward compatible)
    assertNull(outputArrayStruct.getString("subfield2"));

    // Verify the schema structure has both fields
    assertEquals(2, outputArrayStruct.schema().fields().size());
    assertNotNull(outputArrayStruct.schema().field("subfield1"));
    assertNotNull(outputArrayStruct.schema().field("subfield2"));

    // Verify the new field is optional (nullable)
    Field subfield2 = outputArrayStruct.schema().field("subfield2");
    assertTrue(subfield2.schema().isOptional());
  }


}
