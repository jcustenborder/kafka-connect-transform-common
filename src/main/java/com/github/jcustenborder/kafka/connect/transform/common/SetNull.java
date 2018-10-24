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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class SetNull<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(SetNull.class);

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  PatternRenameConfig config;

  @Override
  public void configure(Map<String, ?> settings) {


  }

  @Override
  public void close() {

  }

  @Title("SetNull(Key)")
  @Description("This transformation will will set the key and keySchema of a message to null.")
  @DocumentationTip("This transformation is used to manipulate the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends SetNull<R> {

    @Override
    public R apply(R r) {
      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          null,
          null,
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("SetNull(Value)")
  @Description("This transformation will will set the value and valueSchema of a message to null.")
  @DocumentationTip("This transformation is used to manipulate the Value of the record.")
  public static class Value<R extends ConnectRecord<R>> extends SetNull<R> {
    @Override
    public R apply(R r) {
      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          null,
          null,
          r.timestamp()
      );
    }
  }
}
