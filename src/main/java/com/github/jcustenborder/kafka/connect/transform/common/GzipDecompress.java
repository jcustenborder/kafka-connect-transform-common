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
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Title("GzipDecompress")
@Description("This transformation is used to gzip-decompress byte arrays.")
public abstract class GzipDecompress<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
  protected GzipDecompress(boolean isKey) {
    super(isKey);
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void configure(Map<String, ?> map) {

  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue process(R record, SchemaAndValue input) {
    if (input.value() instanceof ByteBuffer) {
      final Schema inputSchema = null == input.schema() ? Schema.BYTES_SCHEMA : input.schema();
      if (Schema.Type.BYTES == inputSchema.type()) {
        return processBytes(record, inputSchema, bytes((ByteBuffer) input.value()));
      }
    }
    return super.process(record, input);
  }

  static byte[] bytes(ByteBuffer input) {
    final ByteBuffer buffer = input.slice();
    final byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    if (null == input) {
      return new SchemaAndValue(inputSchema, null);
    }

    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(input)) {
      try (GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
          gzipInputStream.transferTo(outputStream);
          return new SchemaAndValue(inputSchema, outputStream.toByteArray());
        }
      }
    } catch (IOException ex) {
      throw new DataException(ex);
    }
  }

  public static class Key<R extends ConnectRecord<R>> extends GzipDecompress<R> {
    public Key() {
      super(true);
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends GzipDecompress<R> {
    public Value() {
      super(false);
    }
  }
}
