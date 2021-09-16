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

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.jcustenborder.kafka.connect.transform.common.GenericAssertions.assertMap;
import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class PatternReplaceTest extends TransformationTest {
  final static String TOPIC = "test";

  protected PatternReplaceTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void schemaLess() {
    this.transformation.configure(
        ImmutableMap.of(
          PatternReplaceConfig.FIELD_NAME_CONF, "firstname",
          PatternReplaceConfig.VALUE_PATTERN_CONF, "\\.",
          PatternReplaceConfig.VALUE_REPLACEMENT_CONF, "_"
        )
    );

    final Map<String, Object> input = ImmutableMap.of(
        "firstname", "example.one",
        "lastname", "user.two"
    );
    final Map<String, Object> expected = ImmutableMap.of(
        "firstname", "example_one",
        "lastname", "user.two"
    );

    final Object key = isKey ? input : null;
    final Object value = isKey ? null : input;
    final Schema keySchema = null;
    final Schema valueSchema = null;

    final SinkRecord inputRecord = new SinkRecord(
        TOPIC,
        1,
        keySchema,
        key,
        valueSchema,
        value,
        1234L
    );
    final SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertNotNull(outputRecord);
    @SuppressWarnings("unchecked")
    final Map<String, Object> actual = (Map<String, Object>) (isKey ? outputRecord.key() : outputRecord.value());
    assertMap(expected, actual, "");
  }

  @Test
  public void prefixed() {
    this.transformation.configure(
        ImmutableMap.of(
          PatternReplaceConfig.FIELD_NAME_CONF, "firstname",
          PatternReplaceConfig.VALUE_PATTERN_CONF, "^prefixed",
          PatternReplaceConfig.VALUE_REPLACEMENT_CONF, ""
        )
    );
    System.err.println("FFS!");

    Schema inputSchema = SchemaBuilder.struct()
        .name("testing")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA);
    Struct inputStruct = new Struct(inputSchema)
        .put("firstname", "prefixedexample")
        .put("lastname", "prefixeduser");

    final Object key = isKey ? inputStruct : null;
    final Object value = isKey ? null : inputStruct;
    final Schema keySchema = isKey ? inputSchema : null;
    final Schema valueSchema = isKey ? null : inputSchema;

    final SinkRecord inputRecord = new SinkRecord(
        TOPIC,
        1,
        keySchema,
        key,
        valueSchema,
        value,
        1234L
    );
    final SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertNotNull(outputRecord);

    final Schema actualSchema = isKey ? outputRecord.keySchema() : outputRecord.valueSchema();
    final Struct actualStruct = (Struct) (isKey ? outputRecord.key() : outputRecord.value());

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("testing")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA);
    Struct expectedStruct = new Struct(expectedSchema)
        .put("firstname", "example")
        .put("lastname", "prefixeduser");

    assertSchema(expectedSchema, actualSchema);
    assertStruct(expectedStruct, actualStruct);
  }

  @Test
  public void multiple() {
    this.transformation.configure(
        ImmutableMap.of(
          PatternReplaceConfig.FIELD_NAME_CONF, "firstname",
          PatternReplaceConfig.VALUE_PATTERN_CONF, "/",
          PatternReplaceConfig.VALUE_REPLACEMENT_CONF, "-"
        )
    );
    System.err.println("FFS!");

    Schema inputSchema = SchemaBuilder.struct()
        .name("testing")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA);
    Struct inputStruct = new Struct(inputSchema)
        .put("firstname", "this/has/multiple/tokens/to/replace")
        .put("lastname", "prefixeduser");

    final Object key = isKey ? inputStruct : null;
    final Object value = isKey ? null : inputStruct;
    final Schema keySchema = isKey ? inputSchema : null;
    final Schema valueSchema = isKey ? null : inputSchema;

    final SinkRecord inputRecord = new SinkRecord(
        TOPIC,
        1,
        keySchema,
        key,
        valueSchema,
        value,
        1234L
    );
    final SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertNotNull(outputRecord);

    final Schema actualSchema = isKey ? outputRecord.keySchema() : outputRecord.valueSchema();
    final Struct actualStruct = (Struct) (isKey ? outputRecord.key() : outputRecord.value());

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("testing")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA);
    Struct expectedStruct = new Struct(expectedSchema)
        .put("firstname", "this-has-multiple-tokens-to-replace")
        .put("lastname", "prefixeduser");

    assertSchema(expectedSchema, actualSchema);
    assertStruct(expectedStruct, actualStruct);
  }


  public static class KeyTest<R extends ConnectRecord<R>> extends PatternReplaceTest {
    protected KeyTest() {
      super(true);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new PatternReplace.Key<SinkRecord>();
    }
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends PatternReplaceTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new PatternReplace.Value<SinkRecord>();
    }
  }
}
