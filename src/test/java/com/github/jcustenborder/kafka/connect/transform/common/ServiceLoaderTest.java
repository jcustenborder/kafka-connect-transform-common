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

import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceLoaderTest {
  static final Set<String> EXPECTED_PROVIDERS = new HashSet<>(Arrays.asList(
      "com.github.jcustenborder.kafka.connect.transform.common.AdjustPrecisionAndScale$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.AdjustPrecisionAndScale$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ChangeTopicCase",
      "com.github.jcustenborder.kafka.connect.transform.common.Debug",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractXPath$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ExtractXPath$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.LowerCaseTopic",
      "com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternMapString$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternMapString$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.SetNull$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.SetNull$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.TimestampNow",
      "com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.ToLong$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.ToLong$Value",
      "com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Key",
      "com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Value"
  ));

  @Test
  public void serviceLoaderDiscoversTransformationProviders() {
    Set<String> actual = new HashSet<>();
    for (Transformation<?> transformation : ServiceLoader.load(Transformation.class)) {
      actual.add(transformation.getClass().getName());
    }

    assertTrue(actual.containsAll(EXPECTED_PROVIDERS), "Missing providers: " + missingProviders(actual));
    assertFalse(actual.contains("com.github.jcustenborder.kafka.connect.transform.common.AdjustPrecisionAndScale"));
    assertFalse(actual.contains("com.github.jcustenborder.kafka.connect.transform.common.HeaderToField"));
    assertFalse(actual.contains("com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision"));
  }

  static Set<String> missingProviders(Set<String> actual) {
    Set<String> missing = new HashSet<>(EXPECTED_PROVIDERS);
    missing.removeAll(actual);
    return missing;
  }
}
