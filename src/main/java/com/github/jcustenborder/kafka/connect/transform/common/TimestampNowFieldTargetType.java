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
