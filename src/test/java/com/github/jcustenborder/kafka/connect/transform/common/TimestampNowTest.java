package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimestampNowTest {

  @Test
  public void test() {
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        null,
        "",
        null,
        "",
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE
    );
    final Long expectedTimestamp = 1537808219123L;
    TimestampNow<SinkRecord> transform = new TimestampNow<>();
    transform.time = mock(Time.class);
    when(transform.time.milliseconds()).thenReturn(expectedTimestamp);
    final SinkRecord actual = transform.apply(input);
    assertEquals(expectedTimestamp, actual.timestamp(), "Timestamp should match.");
    verify(transform.time, times(1)).milliseconds();
  }


}
