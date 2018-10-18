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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;

public abstract class PatternFilter<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(PatternFilter.class);

  @Override
  public ConfigDef config() {
    return PatternFilterConfig.config();
  }

  PatternFilterConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new PatternFilterConfig(settings);
  }

  @Override
  public void close() {

  }

  R filter(R record, Struct struct) {
    for (Field field : struct.schema().fields()) {
      if (this.config.fields.contains(field.name())) {
        if (field.schema().type() == Schema.Type.STRING) {
          String input = struct.getString(field.name());
          if (null != input) {
            Matcher matcher = this.config.pattern.matcher(input);
            if (matcher.matches()) {
              return null;
            }
          }
        }
      }
    }
    return record;
  }

  R filter(R record, Map map) {
    for (Object field : map.keySet()) {
      if (this.config.fields.contains(field)) {
        Object value = map.get(field);

        if (value instanceof String) {
          String input = (String) value;
          Matcher matcher = this.config.pattern.matcher(input);
          if (matcher.matches()) {
            return null;
          }
        }
      }
    }

    return record;
  }


  R filter(R record, final boolean key) {
    final SchemaAndValue input = key ?
        new SchemaAndValue(record.keySchema(), record.key()) :
        new SchemaAndValue(record.valueSchema(), record.value());
    final R result;
    if (input.schema() != null) {
      if (Schema.Type.STRUCT == input.schema().type()) {
        result = filter(record, (Struct) input.value());
      } else if (Schema.Type.MAP == input.schema().type()) {
        result = filter(record, (Map) input.value());
      } else {
        result = record;
      }
    } else if (input.value() instanceof Map) {
      result = filter(record, (Map) input.value());
    } else {
      result = record;
    }

    return result;
  }

  @Title("PatternFilter(Key)")
  @Description("This transformation is used to filter records based on a regular expression.")
  @DocumentationTip("This transformation is used to filter records based on fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends PatternFilter<R> {
    @Override
    public R apply(R r) {
      return filter(r, true);
    }
  }

  @Title("PatternFilter(Value)")
  @Description("This transformation is used to filter records based on a regular expression.")
  @DocumentationTip("This transformation is used to filter records based on fields in the Value of the record.")
  public static class Value<R extends ConnectRecord<R>> extends PatternFilter<R> {
    @Override
    public R apply(R r) {
      return filter(r, false);
    }
  }
}
