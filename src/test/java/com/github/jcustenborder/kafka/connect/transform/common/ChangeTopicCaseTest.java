package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ChangeTopicCaseTest extends TransformationTest {
  protected ChangeTopicCaseTest() {
    super(false);
  }

  @Override
  protected Transformation<SinkRecord> create() {
    return new ChangeTopicCase<>();
  }

  SinkRecord record(String topic) {
    return new SinkRecord(
        topic,
        1,
        null,
        null,
        null,
        null,
        12345L
    );

  }

  static class TestCase {
    final CaseFormat from;
    final String input;
    final CaseFormat to;
    final String expected;

    TestCase(CaseFormat from, String input, CaseFormat to, String expected) {
      this.from = from;
      this.input = input;
      this.to = to;
      this.expected = expected;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("from", this.from)
          .add("to", this.to)
          .add("input", this.input)
          .add("expected", this.expected)
          .toString();
    }
  }

  static TestCase of(CaseFormat from, String input, CaseFormat to, String expected) {
    return new TestCase(from, input, to, expected);
  }

  @TestFactory
  public Stream<DynamicTest> apply() {
    return Arrays.asList(
        of(CaseFormat.UPPER_UNDERSCORE, "TOPIC_NAME", CaseFormat.LOWER_CAMEL, "topicName"),
        of(CaseFormat.LOWER_CAMEL, "topicName", CaseFormat.UPPER_UNDERSCORE, "TOPIC_NAME"),
        of(CaseFormat.LOWER_HYPHEN, "topic-name", CaseFormat.LOWER_UNDERSCORE, "topic_name")
    ).stream()
        .map(t -> DynamicTest.dynamicTest(t.toString(), () -> {
          final Map<String, String> settings = ImmutableMap.of(
              ChangeTopicCaseConfig.FROM_CONFIG, t.from.toString(),
              ChangeTopicCaseConfig.TO_CONFIG, t.to.toString()
          );
          this.transformation.configure(settings);
          final SinkRecord input = record(t.input);
          final SinkRecord actual = this.transformation.apply(input);
          assertNotNull(actual, "actual should not be null.");
          assertEquals(t.expected, actual.topic(), "topic does not match.");
        }));
  }
}
