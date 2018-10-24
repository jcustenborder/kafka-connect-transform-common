package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetNullTest {

  @Test
  public void test() {
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        Schema.STRING_SCHEMA,
        "key",
        null,
        "",
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE
    );
    final Long expectedTimestamp = 1537808219123L;
    SetNull<SinkRecord> transform = new SetNull.Key<>();
    final SinkRecord actual = transform.apply(input);
    assertNull(actual.key(), "key should be null.");
    assertNull(actual.keySchema(), "keySchema should be null.");
  }


}
