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

import com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugTest {
  Debug<SinkRecord> transform;

  @BeforeEach
  public void before() {
    this.transform = new Debug<>();
    this.transform.configure(ImmutableMap.of());
  }

  @Test
  public void apply() {
    Schema valueSchema = SchemaBuilder.struct()
        .name("foo")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build();


    SinkRecord input = SinkRecordHelper.write("test", Schema.STRING_SCHEMA, "1234", valueSchema, new Struct(valueSchema).put("firstName", "adfs").put("lastName", "asdfas"));
    SinkRecord output = this.transform.apply(input);

  }


}
