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

import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import com.google.common.io.ByteStreams;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;

public abstract class Decompress<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  public Decompress(boolean isKey) {
    super(isKey);
  }


  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  protected abstract InputStream createStream(InputStream input) throws IOException;


  @Override
  public void configure(Map<String, ?> map) {

  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String base64Input) {
    byte[] input = Base64.getDecoder().decode(base64Input);
    Schema bytesSchema = inputSchema.isOptional() ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
    SchemaAndValue compressed = processBytes(record, bytesSchema, input);
    String result = Base64.getEncoder().encodeToString((byte[]) compressed.value());
    return new SchemaAndValue(inputSchema, result);
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    try (InputStream inputStream = new ByteArrayInputStream(input)) {
      try (InputStream decompressStream = createStream(inputStream)) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
          ByteStreams.copy(decompressStream, outputStream);
          return new SchemaAndValue(inputSchema, outputStream.toByteArray());
        }
      }
    } catch (IOException ex) {
      throw new DataException(ex);
    }
  }
}
