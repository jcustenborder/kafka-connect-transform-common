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

import org.apache.kafka.connect.connector.ConnectRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public abstract class GzipDecompress<R extends ConnectRecord<R>> extends Decompress<R> {
  public GzipDecompress(boolean isKey) {
    super(isKey);
  }

  @Override
  protected InputStream createStream(InputStream input) throws IOException {
    return new GZIPInputStream(input);
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
