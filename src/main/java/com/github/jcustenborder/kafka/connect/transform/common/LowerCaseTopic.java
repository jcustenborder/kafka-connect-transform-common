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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

@Title("LowerCaseTopic")
@Description("This transformation is used to change a topic name to be all lower case.")
public class LowerCaseTopic<R extends ConnectRecord<R>> implements Transformation<R> {
  Time time = SystemTime.SYSTEM;

  @Override
  public R apply(R record) {
    return record.newRecord(
        record.topic().toLowerCase(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        record.value(),
        record.timestamp(),
        record.headers()
    );
  }

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
}
