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

public abstract class ExtractXPathTest extends TransformationTest {
  protected ExtractXPathTest(boolean isKey) {
    super(isKey);
  }

  /**
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
  */

  @Test
  public void checkConfig1() {
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//root")
    );
    ExtractXPathConfig xpc = ((ExtractXPath) this.transformation).theConfig();
    assertEquals(xpc.namespaceAware, false);
  }
  @Test
  public void checkConfig2() {
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//ns1:root",
        ExtractXPathConfig.NS_PREFIX_CONFIG, "ns1,ns2,ns3",
        ExtractXPathConfig.NS_LIST_CONFIG, "http://ns1.io/one,http://ns2.io/two,http://ns3.io/three")
    );
    ExtractXPathConfig xpc = ((ExtractXPath) this.transformation).theConfig();
    assertEquals(xpc.namespaceAware, true);
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends ExtractXPathTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new ExtractXPath.Value<>();
    }
  }
}
