/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

import java.time.Instant;
import java.util.Date;

public enum TimestampNowFieldTargetType {
  DATE {
    @Override
    boolean isMatchingSchema(Schema schema) {
      return Timestamp.SCHEMA.type() == getSchema().type() && Timestamp.SCHEMA.name().equals(schema.name());
    }

    @Override
    Schema getSchema() {
      return Timestamp.SCHEMA;
    }

    @Override
    Object getFormattedTimestamp(long timeInMillis) {
      return new Date(timeInMillis);
    }
  },
  UNIX {
    @Override
    boolean isMatchingSchema(Schema schema) {
      return Schema.Type.INT64 == schema.type() && null == schema.name();
    }

    @Override
    Schema getSchema() {
      return Schema.INT64_SCHEMA;
    }

    @Override
    Object getFormattedTimestamp(long timeInMillis) {
      return Instant.ofEpochMilli(timeInMillis).getEpochSecond();
    }
  };

  abstract boolean isMatchingSchema(Schema schema);

  abstract Schema getSchema();

  abstract Object getFormattedTimestamp(long timeInMillis);
}
