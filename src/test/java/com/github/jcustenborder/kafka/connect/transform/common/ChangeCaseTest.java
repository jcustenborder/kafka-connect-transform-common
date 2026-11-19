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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public abstract class ChangeCaseTest extends TransformationTest {
  protected ChangeCaseTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void test() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_UNDERSCORE.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_UNDERSCORE.toString()));
    final Schema inputSchema = makeSchema(CaseFormat.UPPER_UNDERSCORE);
    final Schema expectedSchema = makeSchema(CaseFormat.LOWER_UNDERSCORE);

    final Struct inputStruct = makeStruct(inputSchema, CaseFormat.UPPER_UNDERSCORE);
    final Struct expectedStruct = makeStruct(expectedSchema, CaseFormat.LOWER_UNDERSCORE);

    final SinkRecord inputRecord = record(inputSchema, inputStruct);
    for (int i = 0; i < 50; i++) {
      final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
      assertNotNull(transformedRecord, "transformedRecord should not be null.");
      assertSchema(expectedSchema, isKey ? transformedRecord.keySchema() : transformedRecord.valueSchema());
      assertStruct(expectedStruct, (Struct) (isKey ? transformedRecord.key() : transformedRecord.value()));
    }
  }

  @Test
  public void nullArrayValue() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_UNDERSCORE.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_UNDERSCORE.toString()));
    final Schema inputSchema = makeSchema(CaseFormat.UPPER_UNDERSCORE);
    final Schema expectedSchema = makeSchema(CaseFormat.LOWER_UNDERSCORE);
    final Struct inputStruct = new Struct(inputSchema).put("CONTACTS", null);
    final SinkRecord inputRecord = record(inputSchema, inputStruct);

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(expectedSchema, isKey ? transformedRecord.keySchema() : transformedRecord.valueSchema());
    assertNull(((Struct) (isKey ? transformedRecord.key() : transformedRecord.value())).get("contacts"));
  }

  @Test
  public void string() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_UNDERSCORE.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_UNDERSCORE.toString()));
    final SinkRecord inputRecord = record(Schema.STRING_SCHEMA, "FIRST_NAME");

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertSchema(Schema.STRING_SCHEMA, isKey ? transformedRecord.keySchema() : transformedRecord.valueSchema());
    assertEquals("first_name", isKey ? transformedRecord.key() : transformedRecord.value());
  }

  @Test
  public void schemaLessString() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_UNDERSCORE.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_UNDERSCORE.toString()));
    final SinkRecord inputRecord = record(null, "FIRST_NAME");

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertNull(isKey ? transformedRecord.keySchema() : transformedRecord.valueSchema());
    assertEquals("first_name", isKey ? transformedRecord.key() : transformedRecord.value());
  }

  @Test
  public void schemaLessMapPassthroughField() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_CAMEL.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_CAMEL.toString(),
                    ChangeCaseConfig.PASSTHROUGH_FIELDS_CONFIG, "Labels,Name"));

    final Map<String, Object> labels = new LinkedHashMap<>();
    labels.put("EM_SwappedNames", "False");
    labels.put("OriginalId", "abc123");

    final Map<String, Object> name = new LinkedHashMap<>();
    name.put("ru", "Ivan");
    name.put("en", "Ivan");

    final Map<String, Object> input = new LinkedHashMap<>();
    input.put("Id", "1");
    input.put("Labels", labels);
    input.put("Name", name);

    final SinkRecord inputRecord = record(null, input);
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    @SuppressWarnings("unchecked")
    final Map<String, Object> output =
            (Map<String, Object>) (isKey ? transformedRecord.key() : transformedRecord.value());

    // top-level field names recased
    assertEquals("1", output.get("id"));
    assertNull(output.get("Id"), "old top-level key should be gone.");

    // passthrough field names recased, but their nested data keys preserved verbatim
    @SuppressWarnings("unchecked")
    final Map<String, Object> outLabels = (Map<String, Object>) output.get("labels");
    assertNotNull(outLabels, "passthrough field key should be recased to 'labels'.");
    assertEquals("False", outLabels.get("EM_SwappedNames"));
    assertEquals("abc123", outLabels.get("OriginalId"));

    @SuppressWarnings("unchecked")
    final Map<String, Object> outName = (Map<String, Object>) output.get("name");
    assertNotNull(outName, "passthrough field key should be recased to 'name'.");
    assertEquals("Ivan", outName.get("ru"));
    assertEquals("Ivan", outName.get("en"));
  }

  @Test
  public void schemaLessDeepNesting() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_CAMEL.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_CAMEL.toString(),
                    ChangeCaseConfig.PASSTHROUGH_FIELDS_CONFIG, "DataLabels"));

    // list of maps
    final Map<String, Object> point = new LinkedHashMap<>();
    point.put("MinuteOfPoint", 3);
    point.put("TimerDirection", "up");
    final List<Object> pointInfos = new ArrayList<>(Collections.singletonList(point));

    // map nested in map + a passthrough map deep in the tree
    final Map<String, Object> passLabels = new LinkedHashMap<>();
    passLabels.put("EM_SwappedNames", "False");
    final Map<String, Object> timer = new LinkedHashMap<>();
    timer.put("CurrentTime", 12);
    timer.put("DataLabels", passLabels);

    // list of lists of maps
    final Map<String, Object> deep = new LinkedHashMap<>();
    deep.put("CompetitorId", "c1");
    final List<Object> innerList = new ArrayList<>(Collections.singletonList(deep));
    final List<Object> outerList = new ArrayList<>(Collections.singletonList(innerList));

    final Map<String, Object> input = new LinkedHashMap<>();
    input.put("EventId", "e1");
    input.put("PointInfos", pointInfos);
    input.put("Timer", timer);
    input.put("CompetitorInfos", outerList);

    final SinkRecord inputRecord = record(null, input);
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    @SuppressWarnings("unchecked")
    final Map<String, Object> out =
            (Map<String, Object>) (isKey ? transformedRecord.key() : transformedRecord.value());

    assertEquals("e1", out.get("eventId"));

    // list of maps -> nested keys renamed
    @SuppressWarnings("unchecked")
    final List<Object> outPoints = (List<Object>) out.get("pointInfos");
    @SuppressWarnings("unchecked")
    final Map<String, Object> outPoint = (Map<String, Object>) outPoints.get(0);
    assertEquals(3, outPoint.get("minuteOfPoint"));
    assertEquals("up", outPoint.get("timerDirection"));

    // map nested in map -> renamed; passthrough map key renamed but its keys preserved
    @SuppressWarnings("unchecked")
    final Map<String, Object> outTimer = (Map<String, Object>) out.get("timer");
    assertEquals(12, outTimer.get("currentTime"));
    @SuppressWarnings("unchecked")
    final Map<String, Object> outLabels = (Map<String, Object>) outTimer.get("dataLabels");
    assertNotNull(outLabels, "passthrough field key should be recased to 'dataLabels'.");
    assertEquals("False", outLabels.get("EM_SwappedNames"));

    // list of lists of maps -> deepest keys renamed
    @SuppressWarnings("unchecked")
    final List<Object> outOuter = (List<Object>) out.get("competitorInfos");
    @SuppressWarnings("unchecked")
    final List<Object> outInner = (List<Object>) outOuter.get(0);
    @SuppressWarnings("unchecked")
    final Map<String, Object> outDeep = (Map<String, Object>) outInner.get(0);
    assertEquals("c1", outDeep.get("competitorId"));
  }

  @Test
  public void schemaLessMap() {
    this.transformation.configure(
            ImmutableMap.of(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_CAMEL.toString(),
                    ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_CAMEL.toString()));

    final Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("FirstName", "test");
    nested.put("LastName", "user");

    final Map<String, Object> arrayEntry = new LinkedHashMap<>();
    arrayEntry.put("StreetName", "main");

    final Map<String, Object> input = new LinkedHashMap<>();
    input.put("TransactionId", "abc");
    input.put("AmountBreakdown", nested);
    input.put("Addresses", new ArrayList<>(Collections.singletonList(arrayEntry)));

    final SinkRecord inputRecord = record(null, input);

    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);

    assertNotNull(transformedRecord, "transformedRecord should not be null.");
    assertNull(isKey ? transformedRecord.keySchema() : transformedRecord.valueSchema());

    @SuppressWarnings("unchecked")
    final Map<String, Object> output =
            (Map<String, Object>) (isKey ? transformedRecord.key() : transformedRecord.value());

    assertEquals("abc", output.get("transactionId"));

    @SuppressWarnings("unchecked")
    final Map<String, Object> outNested = (Map<String, Object>) output.get("amountBreakdown");
    assertNotNull(outNested, "nested map should be renamed and present.");
    assertEquals("test", outNested.get("firstName"));
    assertEquals("user", outNested.get("lastName"));

    @SuppressWarnings("unchecked")
    final List<Object> outArray = (List<Object>) output.get("addresses");
    assertNotNull(outArray, "nested array should be renamed and present.");
    @SuppressWarnings("unchecked")
    final Map<String, Object> outArrayEntry = (Map<String, Object>) outArray.get(0);
    assertEquals("main", outArrayEntry.get("streetName"));
  }

  private SinkRecord record(Schema inputSchema, Object input) {
    return new SinkRecord(
            "topic",
            1,
            isKey ? inputSchema : null,
            isKey ? input : null,
            isKey ? null : inputSchema,
            isKey ? null : input,
            1L
    );
  }

  private Schema makeSchema(CaseFormat caseFormat) {
    final Function<String, String> convert = s -> CaseFormat.LOWER_UNDERSCORE.to(caseFormat, s);
    return SchemaBuilder.struct().field(convert.apply("contacts"),
            SchemaBuilder.array(SchemaBuilder.struct()
                    .field(convert.apply("contact"),
                            SchemaBuilder.struct()
                                    .field(convert.apply("first_name"), Schema.STRING_SCHEMA)
                                    .field(convert.apply("last_name"), Schema.STRING_SCHEMA)
                                    .build()
                    ).build()).optional()
    ).build();
  }

  private Struct makeStruct(Schema schema, CaseFormat caseFormat) {
    final Function<String, String> convert = s -> CaseFormat.LOWER_UNDERSCORE.to(caseFormat, s);
    final Schema contacts = schema.fields().get(0).schema().valueSchema();
    final Schema contact = contacts.fields().get(0).schema();
    return new Struct(schema).put(convert.apply("contacts"),
            new ArrayList<>(
                    Collections.singletonList(
                            new Struct(contacts).put(convert.apply("contact"),
                                    new Struct(contact)
                                            .put(convert.apply("first_name"), "test")
                                            .put(convert.apply("last_name"), "user"))
                    )
            )
    );
  }

  public static class KeyTest<R extends ConnectRecord<R>> extends ChangeCaseTest {
    protected KeyTest() {
      super(true);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new ChangeCase.Key<>();
    }
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends ChangeCaseTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new ChangeCase.Value<>();
    }
  }
}
