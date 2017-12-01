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
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public abstract class ExtractTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ExtractTimestamp.class);
  public ExtractTimestampConfig config;

  protected long process(SchemaAndValue schemaAndValue) {
    final long result;
    if (schemaAndValue.value() instanceof Struct) {
      result = processStruct(schemaAndValue);
    } else if (schemaAndValue.value() instanceof Map) {
      result = processMap(schemaAndValue);
    } else {
      throw new UnsupportedOperationException();
    }
    return result;
  }

  private long processMap(SchemaAndValue schemaAndValue) {
    Preconditions.checkState(schemaAndValue.value() instanceof Map, "value must be a map.");
    final Map<String, Object> input = (Map<String, Object>) schemaAndValue.value();
    final Object inputValue = input.get(this.config.fieldName);
    final long result;

    if (inputValue instanceof Date) {
      final Date inputDate = (Date) inputValue;
      result = inputDate.getTime();
    } else if (inputValue instanceof Long) {
      result = (long) inputValue;
    } else if (null == inputValue) {
      throw new DataException(
          String.format("Field '%s' cannot be null.", this.config.fieldName)
      );
    } else {
      throw new DataException(
          String.format("Cannot convert %s to timestamp.", inputValue.getClass().getName())
      );
    }

    return result;
  }

  private long processStruct(SchemaAndValue schemaAndValue) {
    final Struct inputStruct = (Struct) schemaAndValue.value();
    final Field inputField = schemaAndValue.schema().field(this.config.fieldName);

    if (null == inputField) {
      throw new DataException(
          String.format("Schema does not have field '{}'", this.config.fieldName)
      );
    }

    final Schema fieldSchema = inputField.schema();
    final long result;
    if (Schema.Type.INT64 == fieldSchema.type()) {
      final Object fieldValue = inputStruct.get(inputField);

      if (null == fieldValue) {
        throw new DataException(
            String.format("Field '%s' cannot be null.", this.config.fieldName)
        );
      }

      if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
        final Date date = (Date) fieldValue;
        result = date.getTime();
      } else {
        final long timestamp = (long) fieldValue;
        result = timestamp;
      }
    } else {
      throw new DataException(
          String.format("Schema '{}' is not supported.", inputField.schema())
      );
    }

    return result;
  }


  @Override
  public ConfigDef config() {
    return ExtractTimestampConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new ExtractTimestampConfig(settings);
  }


  @Title("ExtractTimestamp(Value)")
  @Description("This transformation is used to use a field from the input data to override the timestamp for the record.")
  public static class Value<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {

    @Override
    public R apply(R r) {
      final long timestamp = process(new SchemaAndValue(r.valueSchema(), r.value()));
      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          r.valueSchema(),
          r.value(),
          timestamp
      );
    }
  }
}
