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
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class ExtractTimestampTest extends TransformationTest {
  protected ExtractTimestampTest(boolean isKey) {
    super(isKey);
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends ExtractTimestampTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new ExtractTimestamp.Value<>();
    }
  }

  static final Date EXPECTED = new Date(1512164613123L);

  @Test
  public void schemaTimestamp() {
    this.transformation.configure(
        ImmutableMap.of(ExtractTimestampConfig.FIELD_NAME_CONFIG, "timestamp")
    );
    final Schema schema = SchemaBuilder.struct()
        .field("timestamp", Timestamp.SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("timestamp", EXPECTED);
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        schema,
        struct,
        1L
    );
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertEquals((Long) EXPECTED.getTime(), transformedRecord.timestamp(), "timestamp does not match.");
  }

  @Test
  public void schemaLong() {
    this.transformation.configure(
        ImmutableMap.of(ExtractTimestampConfig.FIELD_NAME_CONFIG, "timestamp")
    );
    final Schema schema = SchemaBuilder.struct()
        .field("timestamp", Schema.INT64_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("timestamp", EXPECTED.getTime());
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        schema,
        struct,
        1L
    );
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertEquals((Long) EXPECTED.getTime(), transformedRecord.timestamp(), "timestamp does not match.");
  }

  @Test
  public void schemalessDate() {
    this.transformation.configure(
        ImmutableMap.of(ExtractTimestampConfig.FIELD_NAME_CONFIG, "timestamp")
    );
    final Map<String, Object> input = ImmutableMap.of(
        "timestamp", (Object) EXPECTED
    );

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        input,
        1L
    );
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertEquals((Long) EXPECTED.getTime(), transformedRecord.timestamp(), "timestamp does not match.");
  }

  @Test
  public void schemalessTimestamp() {
    this.transformation.configure(
        ImmutableMap.of(ExtractTimestampConfig.FIELD_NAME_CONFIG, "timestamp")
    );
    final Map<String, Object> input = ImmutableMap.of(
        "timestamp", (Object) EXPECTED.getTime()
    );

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        null,
        input,
        1L
    );
    final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
    assertEquals((Long) EXPECTED.getTime(), transformedRecord.timestamp(), "timestamp does not match.");
  }
}
