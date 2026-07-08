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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class GzipCompressionTest {
  @Test
  public void valueRoundTripBytes() throws IOException {
    final byte[] input = "hello gzip value".getBytes(StandardCharsets.UTF_8);
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        Schema.BYTES_SCHEMA,
        input,
        1L
    );

    final Transformation<SinkRecord> compress = new GzipCompress.Value<>();
    compress.configure(Map.of());
    final SinkRecord compressedRecord = compress.apply(inputRecord);
    final byte[] compressed = (byte[]) compressedRecord.value();

    assertFalse(Arrays.equals(input, compressed));
    assertArrayEquals(input, gunzip(compressed));
    assertEquals(Schema.BYTES_SCHEMA, compressedRecord.valueSchema());

    final Transformation<SinkRecord> decompress = new GzipDecompress.Value<>();
    decompress.configure(Map.of());
    final SinkRecord decompressedRecord = decompress.apply(compressedRecord);

    assertArrayEquals(input, (byte[]) decompressedRecord.value());
    assertEquals(Schema.BYTES_SCHEMA, decompressedRecord.valueSchema());
  }

  @Test
  public void valueRoundTripByteBuffer() {
    final byte[] input = "hello gzip byte buffer".getBytes(StandardCharsets.UTF_8);
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        Schema.BYTES_SCHEMA,
        ByteBuffer.wrap(input),
        1L
    );

    final Transformation<SinkRecord> compress = new GzipCompress.Value<>();
    compress.configure(Map.of());
    final SinkRecord compressedRecord = compress.apply(inputRecord);
    final byte[] compressed = (byte[]) compressedRecord.value();

    final SinkRecord compressedByteBufferRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        Schema.BYTES_SCHEMA,
        ByteBuffer.wrap(compressed),
        1L
    );
    final Transformation<SinkRecord> decompress = new GzipDecompress.Value<>();
    decompress.configure(Map.of());
    final SinkRecord decompressedRecord = decompress.apply(compressedByteBufferRecord);

    assertArrayEquals(input, (byte[]) decompressedRecord.value());
    assertEquals(Schema.BYTES_SCHEMA, decompressedRecord.valueSchema());
  }

  @Test
  public void keyRoundTripBytes() {
    final byte[] input = "hello gzip key".getBytes(StandardCharsets.UTF_8);
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        Schema.BYTES_SCHEMA,
        input,
        Schema.STRING_SCHEMA,
        "value",
        1L
    );

    final Transformation<SinkRecord> compress = new GzipCompress.Key<>();
    compress.configure(Map.of());
    final SinkRecord compressedRecord = compress.apply(inputRecord);
    final byte[] compressed = (byte[]) compressedRecord.key();

    assertFalse(Arrays.equals(input, compressed));
    assertEquals(Schema.BYTES_SCHEMA, compressedRecord.keySchema());
    assertEquals("value", compressedRecord.value());

    final Transformation<SinkRecord> decompress = new GzipDecompress.Key<>();
    decompress.configure(Map.of());
    final SinkRecord decompressedRecord = decompress.apply(compressedRecord);

    assertArrayEquals(input, (byte[]) decompressedRecord.key());
    assertEquals(Schema.BYTES_SCHEMA, decompressedRecord.keySchema());
    assertEquals("value", decompressedRecord.value());
  }

  static byte[] gunzip(byte[] input) throws IOException {
    try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(input))) {
      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        gzipInputStream.transferTo(outputStream);
        return outputStream.toByteArray();
      }
    }
  }
}
