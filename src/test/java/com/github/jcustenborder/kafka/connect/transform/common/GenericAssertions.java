/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.base.Strings;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericAssertions {
  private GenericAssertions() {

  }

  static class MapDifferenceSupplier implements Supplier<String> {
    final MapDifference<?, ?> mapDifference;
    final String method;

    public MapDifferenceSupplier(MapDifference<?, ?> mapDifference, String method) {
      this.mapDifference = mapDifference;
      this.method = method;
    }

    @Override
    public String get() {
      try (Writer w = new StringWriter()) {
        try (BufferedWriter writer = new BufferedWriter(w)) {
          writer.append(String.format("Map for actual.%s() does not match expected.%s().", this.method, this.method));
          writer.newLine();
          Map<?, ? extends MapDifference.ValueDifference<?>> differences = mapDifference.entriesDiffering();
          if (!differences.isEmpty()) {
            writer.append("Keys with Differences");
            writer.newLine();
            for (Map.Entry<?, ? extends MapDifference.ValueDifference<?>> kvp : differences.entrySet()) {
              writer.append("  ");
              writer.append(kvp.getKey().toString());
              writer.newLine();

              writer.append("    expected:");
              writer.append(kvp.getValue().leftValue().toString());
              writer.newLine();

              writer.append("    actual:");
              writer.append(kvp.getValue().rightValue().toString());
              writer.newLine();
            }
          }

          Map<?, ?> entries = mapDifference.entriesOnlyOnLeft();
          writeEntries(writer, "Only in expected map", entries);

          Map<?, ?> onlyInActual = mapDifference.entriesOnlyOnRight();
          writeEntries(writer, "Only in actual map", onlyInActual);
        }
        return w.toString();
      } catch (IOException ex) {
        throw new IllegalStateException(ex);
      }
    }

    private void writeEntries(BufferedWriter writer, String header, Map<?, ?> entries) throws IOException {
      if (!entries.isEmpty()) {
        writer.append(header);
        writer.newLine();

        for (Map.Entry<?, ?> kvp : entries.entrySet()) {
          writer.append("  ");
          writer.append(kvp.getKey().toString());
          writer.append(": ");
          writer.append(kvp.getValue().toString());
          writer.newLine();
        }
        writer.newLine();
      }
    }
  }

  static void assertMap(Map<String, ?> expected, Map<String, ?> actual, String message) {
    if (null == expected && null == actual) {
      return;
    }

    String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
    assertNotNull(expected, prefix + "expected cannot be null");
    assertNotNull(actual, prefix + "actual cannot be null");
    MapDifference<String, ?> mapDifference = Maps.difference(expected, actual);
    assertTrue(mapDifference.areEqual(), new MapDifferenceSupplier(mapDifference, prefix));
  }
}
