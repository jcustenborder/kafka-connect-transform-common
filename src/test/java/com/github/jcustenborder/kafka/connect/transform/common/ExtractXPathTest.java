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
import java.io.File;
import com.google.common.io.Files;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class ExtractXPathTest extends TransformationTest {
  protected ExtractXPathTest(boolean isKey) {
    super(isKey);
  }
  private static final Logger log = LoggerFactory.getLogger(ExtractXPathTest.class);

  
  @Test
  public void SOAPEnvelope() throws UnsupportedEncodingException, IOException {
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//ns1:Transaction/ns1:Transaction",
        ExtractXPathConfig.NS_PREFIX_CONFIG, "soap,ns1",
        ExtractXPathConfig.NS_LIST_CONFIG, "http://www.w3.org/2003/05/soap-envelope/,http://test.confluent.io/test/abc.xsd"
      )
    );
    Schema schema = SchemaBuilder.struct()
      .name("testing")
      .field("in", Schema.STRING_SCHEMA)
      .build();

    final byte[] expectedOut = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/Transaction.xml"));
    final String expected =  new String (expectedOut);
    
    final byte[] input = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/SOAPEnvelope1.xml"));
    String soapEnvelope = new String (input);
    Struct struct = new Struct(schema)
      .put("in", soapEnvelope);

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
    assertNotNull(outputRecord);
    final Schema actualSchema = isKey ? outputRecord.keySchema() : outputRecord.valueSchema();
    final Struct actualStruct = (Struct) (isKey ? outputRecord.key() : outputRecord.value());

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("testing")
        .field("in", Schema.STRING_SCHEMA)
        .field("out", Schema.STRING_SCHEMA);
    Struct expectedStruct = new Struct(expectedSchema)
        .put("in", soapEnvelope)
        .put("out", expected);

    assertSchema(expectedSchema, actualSchema);
    assertStruct(expectedStruct, actualStruct);
  
  }
  @Test
  public void SOAPEnvelopeByte() throws UnsupportedEncodingException, IOException {
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.XPATH_CONFIG, "//ns1:Transaction/ns1:Transaction",
        ExtractXPathConfig.NS_PREFIX_CONFIG, "soap,ns1",
        ExtractXPathConfig.NS_LIST_CONFIG, "http://www.w3.org/2003/05/soap-envelope/,http://test.confluent.io/test/abc.xsd"
      )
    );

    final byte[] expected = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/Transaction.xml"));
    
    final byte[] input = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/SOAPEnvelope1.xml"));

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        Schema.BYTES_SCHEMA,
        input,
        1L
    );

    log.trace("Input: {}", new String(input));
    SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertNotNull(outputRecord);
    final Schema actualSchema = isKey ? outputRecord.keySchema() : outputRecord.valueSchema();
    final byte[] actualStruct = (byte[]) (isKey ? outputRecord.key() : outputRecord.value());
    log.trace("Output: {}", new String(actualStruct));

    assertSchema(Schema.BYTES_SCHEMA, actualSchema);
    assertEquals(actualStruct.length, expected.length);
  
  }
  @Test
  public void SOAPEnvelopeStructByte() throws UnsupportedEncodingException, IOException {
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//ns1:Transaction/ns1:Transaction",
        ExtractXPathConfig.NS_PREFIX_CONFIG, "soap,ns1",
        ExtractXPathConfig.NS_LIST_CONFIG, "http://www.w3.org/2003/05/soap-envelope/,http://test.confluent.io/test/abc.xsd"
      )
    );
    Schema schema = SchemaBuilder.struct()
      .name("testing")
      .field("in", Schema.BYTES_SCHEMA)
      .build();

    final byte[] expected = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/Transaction.xml"));
    
    final byte[] input = Files.toByteArray(new File("src/test/resources/com/github/jcustenborder/kafka/connect/transform/common/ExtractXPath/SOAPEnvelope1.xml"));
    Struct struct = new Struct(schema)
      .put("in", input);

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
    assertNotNull(outputRecord);
    final Schema actualSchema = isKey ? outputRecord.keySchema() : outputRecord.valueSchema();
    final Struct actualStruct = (Struct) (isKey ? outputRecord.key() : outputRecord.value());

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("testing")
        .field("in", Schema.BYTES_SCHEMA)
        .field("out", Schema.BYTES_SCHEMA);
    Struct expectedStruct = new Struct(expectedSchema)
        .put("in", input)
        .put("out", expected);

    assertSchema(expectedSchema, actualSchema);
    assertStruct(expectedStruct, actualStruct);
  
  }


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

  @Test
  public void xxeExternalEntityIsNotResolved() throws IOException {
    // A crafted record references a local file via an external entity. With the parser hardened
    // (DOCTYPE rejected), the file must NOT be read into the output. On an unhardened parser this
    // would leak the file contents.
    File secret = File.createTempFile("xxe-secret", ".txt");
    java.nio.file.Files.write(secret.toPath(), "SECRET-DO-NOT-LEAK".getBytes());

    String xml = "<?xml version=\"1.0\"?>\n"
        + "<!DOCTYPE r [ <!ENTITY xxe SYSTEM \"file://" + secret.getAbsolutePath() + "\"> ]>\n"
        + "<r>&xxe;</r>";

    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//r")
    );

    Schema schema = SchemaBuilder.struct()
      .name("testing")
      .field("in", Schema.STRING_SCHEMA)
      .build();
    Struct struct = new Struct(schema).put("in", xml);

    final SinkRecord inputRecord = new SinkRecord("topic", 1, null, null, schema, struct, 1L);

    // With the parser hardened, the DOCTYPE is rejected, so the entity is never expanded. The
    // transform then either returns a record without the file contents, or fails the record
    // (e.g. DataException). Either way the secret must never surface. On an unhardened parser
    // apply() would succeed and the output would contain the file contents.
    try {
      SinkRecord outputRecord = this.transformation.apply(inputRecord);
      String dump = String.valueOf(outputRecord.key()) + String.valueOf(outputRecord.value());
      assertFalse(dump.contains("SECRET-DO-NOT-LEAK"),
          "External entity was resolved - XXE protection is not in effect");
    } catch (org.apache.kafka.connect.errors.DataException expected) {
      // Record rejected because the malicious XML failed to parse - nothing was read.
      assertFalse(String.valueOf(expected.getMessage()).contains("SECRET-DO-NOT-LEAK"),
          "External entity contents leaked into the error");
    }
  }

  @Test
  public void doctypeAllowedWhenSecureProcessingDisabled() {
    // Opt-out: secure.processing.enabled=false restores the previous (permissive) behavior, so a
    // DOCTYPE with an internal entity is processed and expanded. Uses an internal entity so the
    // result does not depend on the JVM's accessExternalDTD default.
    this.transformation.configure(
      ImmutableMap.of(
        ExtractXPathConfig.IN_FIELD_CONFIG, "in",
        ExtractXPathConfig.OUT_FIELD_CONFIG, "out",
        ExtractXPathConfig.XPATH_CONFIG, "//r",
        ExtractXPathConfig.SECURE_PROCESSING_CONFIG, "false")
    );

    String xml = "<?xml version=\"1.0\"?>\n"
        + "<!DOCTYPE r [ <!ENTITY foo \"INTERNAL-VALUE\"> ]>\n"
        + "<r>&foo;</r>";

    Schema schema = SchemaBuilder.struct()
      .name("testing")
      .field("in", Schema.STRING_SCHEMA)
      .build();
    Struct struct = new Struct(schema).put("in", xml);

    final SinkRecord inputRecord = new SinkRecord("topic", 1, null, null, schema, struct, 1L);
    SinkRecord outputRecord = this.transformation.apply(inputRecord);
    assertNotNull(outputRecord);

    final Struct actualStruct = (Struct) (isKey ? outputRecord.key() : outputRecord.value());
    assertTrue(
        String.valueOf(actualStruct.get("out")).contains("INTERNAL-VALUE"),
        "DOCTYPE/internal entity should be processed when secure.processing.enabled=false");
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
