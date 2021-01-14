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

import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class Debug<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(Debug.class);

  void addConnectRecord(Map<String, Object> debugContent, ConnectRecord r) {
    debugContent.put("topic", r.topic());
    debugContent.put("kafkaPartition", r.kafkaPartition());
    debugContent.put("timestamp", r.timestamp());
  }

  void addSinkRecord(Map<String, Object> debugContent, SinkRecord r) {
    debugContent.put("timestampType", r.timestampType());
    debugContent.put("kafkaOffset", r.kafkaOffset());
  }

  void addSourceRecord(Map<String, Object> debugContent, SourceRecord r) {
    debugContent.put("sourcePartition", r.sourcePartition());
    debugContent.put("sourceOffset", r.sourceOffset());
  }

  void addKey(Map<String, Object> debugContent, R record) {
    Object result = record.key();
    debugContent.put("key", result);
  }

  void addValue(Map<String, Object> debugContent, R record) {
    Object result = record.value();
    debugContent.put("value", result);
  }

  @Override
  public R apply(R r) {
    try {
      Map<String, Object> debugContent = new LinkedHashMap<>();
      addConnectRecord(debugContent, r);
      if (r instanceof SinkRecord) {
        SinkRecord sinkRecord = (SinkRecord) r;
        addSinkRecord(debugContent, sinkRecord);
      } else if (r instanceof SourceRecord) {
        SourceRecord sourceRecord = (SourceRecord) r;
        addSourceRecord(debugContent, sourceRecord);
      }
      addKey(debugContent, r);
      addValue(debugContent, r);
      log.info("\n{}", ObjectMapperFactory.INSTANCE.writeValueAsString(debugContent));
    } catch (Exception ex) {
      log.error("Exception while generating debug content.", ex);
    }

    return r;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    ObjectMapperFactory.INSTANCE.configure(SerializationFeature.INDENT_OUTPUT, true);
  }
}
