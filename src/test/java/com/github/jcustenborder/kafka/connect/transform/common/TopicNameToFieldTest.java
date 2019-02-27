package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TopicNameToFieldTest extends TransformationTest {


  protected TopicNameToFieldTest(boolean isKey) {
    super(isKey);
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends TopicNameToFieldTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new TopicNameToField.Value<>();
    }
  }

  @Test
  public void struct() {
    Schema schema = SchemaBuilder.struct()
        .field("test", Schema.STRING_SCHEMA)
        .build();


    SchemaAndValue input = new SchemaAndValue(schema, new Struct(schema).put("test", "test"));
    SinkRecord record = write("testing", new SchemaAndValue(Schema.STRING_SCHEMA, "foo"), input);

    this.transformation.configure(
        ImmutableMap.of(TopicNameToFieldConfig.FIELD_CONFIG, "topicName")
    );
    SinkRecord actual = this.transformation.apply(record);
    assertNotNull(actual, "actual cannot be null.");
    assertTrue(actual.value() instanceof Struct, "value() should be a struct.");
    Struct struct = (Struct) actual.value();
    assertEquals("testing", struct.getString("topicName"));



  }

}
