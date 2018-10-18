package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PatternFilterTest {
  public PatternFilter.Value transform;

  @BeforeEach
  public void before() {
    this.transform = new PatternFilter.Value();
    this.transform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^filter$"
        )
    );
  }

  SinkRecord map(String value) {
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        null,
        ImmutableMap.of("input", value),
        1234L
    );
  }

  SinkRecord struct(String value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }

  @Test
  public void filtered() {
    assertNull(this.transform.apply(struct("filter")));
    assertNull(this.transform.apply(map("filter")));
  }

  @Test
  public void notFiltered() {
    assertNotNull(this.transform.apply(struct("ok")));
    assertNotNull(this.transform.apply(map("ok")));
  }

}
