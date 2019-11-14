/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Title("SchemaNameToTopic")
@Description("This transformation is used to take the name from the schema for the key or value and" +
    " replace the topic with this value.")
public abstract class SchemaNameToTopic<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(SchemaNameToTopic.class);

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }

  public static class Key<R extends ConnectRecord<R>> extends SchemaNameToTopic<R> {
    @Override
    public R apply(R r) {

      return r.newRecord(
          r.keySchema().name(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          r.valueSchema(),
          r.value(),
          r.timestamp(),
          r.headers()
      );
    }
  }


  public static class Value<R extends ConnectRecord<R>> extends SchemaNameToTopic<R> {
    @Override
    public R apply(R r) {

      return r.newRecord(
          r.valueSchema().name(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          r.valueSchema(),
          r.value(),
          r.timestamp(),
          r.headers()
      );
    }
  }


}
