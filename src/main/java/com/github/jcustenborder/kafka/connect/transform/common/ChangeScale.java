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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class ChangeScale<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ChangeScale.class);
  private ChangeScaleConfig config;

  @Override
  public ConfigDef config() {
    return ChangeScaleConfig.config();
  }

  @Override
  public void close() {

  }

  Map<Schema, Schema> schemaLookup = new HashMap<>();

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new ChangeScaleConfig(map);
  }

  @Override
  protected SchemaAndValue processStruct(R record, final Schema inputSchema, final Struct input) {
    final Schema outputSchema = this.schemaLookup.computeIfAbsent(inputSchema, schema -> {
          final SchemaBuilder builder = SchemaBuilder.struct();
          return builder.build();
        }
    );

    for (Field inputField : inputSchema.fields()) {
      if (!this.config.fields.contains(inputField.name())) {
        log.trace("processStruct() - skipping field '{}'", inputField.name());
        continue;
      }
      final Field outputField = outputSchema.field(inputField.name());





    }


    return super.processStruct(record, inputSchema, input);
  }
}
