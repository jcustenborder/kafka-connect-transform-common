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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class ToJSON<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ToJSON.class);


  ToJSONConfig config;

  @Override
  public ConfigDef config() {
    return ToJSONConfig.config();
  }

  @Override
  public void close() {

  }

  JsonConverter converter = new JsonConverter();

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new ToJSONConfig(settings);
    Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
    settingsClone.put(ToJSONConfig.SCHEMAS_ENABLE_CONFIG, this.config.schemasEnable);
    this.converter.configure(settingsClone, false);
  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    return new SchemaAndValue(inputSchema, input);
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    return new SchemaAndValue(inputSchema, input);
  }

  SchemaAndValue schemaAndValue(Schema inputSchema, Object input) {
    final byte[] buffer = this.converter.fromConnectData("dummy", inputSchema, input);
    final Schema schema;
    final Object value;

    switch (this.config.outputSchema) {
      case STRING:
        value = new String(buffer, Charsets.UTF_8);
        schema = Schema.OPTIONAL_STRING_SCHEMA;
        break;
      case BYTES:
        value = buffer;
        schema = Schema.OPTIONAL_BYTES_SCHEMA;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Schema type (%s)'%s' is not supported.",
                ToJSONConfig.OUTPUT_SCHEMA_CONFIG,
                this.config.outputSchema
            )
        );
    }

    return new SchemaAndValue(schema, value);
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    return schemaAndValue(null, input);
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    return schemaAndValue(inputSchema, input);
  }

  @Title("ToJson(Key)")
  @Description("This transformation is used to take structured data such as AVRO and output it as " +
      "JSON by way of the JsonConverter built into Kafka Connect.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends ToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("ToJson(Value)")
  @Description("This transformation is used to take structured data such as AVRO and output it as " +
      "JSON by way of the JsonConverter built into Kafka Connect.")
  public static class Value<R extends ConnectRecord<R>> extends ToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }

}
