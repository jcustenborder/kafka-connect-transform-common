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

public class LowerCaseTopicTest {

  @Test
  public void test() {
    final SinkRecord input = new SinkRecord(
        "TeSt",
        1,
        null,
        "",
        null,
        "",
        1234123L,
        12341312L,
        TimestampType.NO_TIMESTAMP_TYPE
    );
    LowerCaseTopic<SinkRecord> transform = new LowerCaseTopic<>();
    final SinkRecord actual = transform.apply(input);
    assertEquals("test", actual.topic(), "Topic should match.");
  }


}
