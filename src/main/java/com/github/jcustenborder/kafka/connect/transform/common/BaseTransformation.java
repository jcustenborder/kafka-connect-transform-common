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
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public abstract class BaseTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  protected abstract SchemaAndValue processStruct(R record, SchemaAndValue schemaAndValue);

  protected abstract SchemaAndValue processMap(R record, SchemaAndValue schemaAndValue);

  protected SchemaAndValue process(R record, SchemaAndValue schemaAndValue) {
    final SchemaAndValue result;
    if (schemaAndValue.value() instanceof Struct) {
      result = processStruct(record, schemaAndValue);
    } else if (schemaAndValue.value() instanceof Map) {
      result = processMap(record, schemaAndValue);
    } else {
      throw new UnsupportedOperationException();
    }
    return result;
  }
}
