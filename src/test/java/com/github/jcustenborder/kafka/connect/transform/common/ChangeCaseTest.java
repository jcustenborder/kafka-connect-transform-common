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
import java.util.function.Function;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    final SinkRecord inputRecord = new SinkRecord("topic", 1, null, null, inputSchema, inputStruct, 1L);
    for (int i = 0; i < 50; i++) {
      final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
      assertNotNull(transformedRecord, "transformedRecord should not be null.");
      assertSchema(expectedSchema, transformedRecord.valueSchema());
      assertStruct(expectedStruct, (Struct) transformedRecord.value());
    }
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
                    ).build())
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
