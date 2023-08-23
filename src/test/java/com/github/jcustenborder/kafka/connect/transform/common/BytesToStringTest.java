package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BytesToStringTest extends TransformationTest {
  protected BytesToStringTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void struct() throws UnsupportedEncodingException {
    this.transformation.configure(
        ImmutableMap.of(BytesToStringConfig.FIELD_CONFIG, "bytes")
    );
    final String expected =  "this is a test";
    Schema schema = SchemaBuilder.struct()
        .field("bytes", Schema.BYTES_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("bytes", expected.getBytes("UTF-8"));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        schema,
        struct,
        1L
    );

    SinkRecord outputRecord = this.transformation.apply(inputRecord);



  }

  @Test
  public void bytes() throws UnsupportedEncodingException {
    this.transformation.configure(
        ImmutableMap.of()
    );
    final String expected =  "this is a test";
    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        Schema.BYTES_SCHEMA,
        expected.getBytes("UTF-8"),
        1L
    );

    SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertEquals(expected, outputRecord.value());
    assertSchema(Schema.STRING_SCHEMA, outputRecord.valueSchema());
  }

  @Test
  public void nullBytes() throws UnsupportedEncodingException {
    this.transformation.configure(
            ImmutableMap.of()
    );
    final String expected =  null;
    final SinkRecord inputRecord = new SinkRecord(
            "topic",
            1,
            null,
            null,
            Schema.OPTIONAL_BYTES_SCHEMA,
            expected,
            1L
    );

    SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertEquals(expected, outputRecord.value());
    assertSchema(Schema.OPTIONAL_STRING_SCHEMA, outputRecord.valueSchema());
  }

  @Test
  public void nullStructFieldBytes() throws UnsupportedEncodingException {
    this.transformation.configure(
            ImmutableMap.of(BytesToStringConfig.FIELD_CONFIG, "bytes")
    );
    final String expected =  null;
    Schema schema = SchemaBuilder.struct()
            .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .build();
    Struct struct = new Struct(schema)
            .put("bytes", expected);

    final SinkRecord inputRecord = new SinkRecord(
            "topic",
            1,
            null,
            null,
            schema,
            struct,
            1L
    );

    SinkRecord outputRecord = this.transformation.apply(inputRecord);
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends BytesToStringTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new BytesToString.Value<>();
    }
  }
}
