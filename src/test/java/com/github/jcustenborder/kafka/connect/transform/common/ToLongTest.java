package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.data.SchemaKey;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static com.github.jcustenborder.kafka.connect.utils.StructHelper.struct;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public abstract class ToLongTest extends TransformationTest {
  protected ToLongTest(boolean isKey) {
    super(isKey);
  }

  @BeforeEach
  public void asdf() {
    this.transformation.configure(
        ImmutableMap.of(ToLongConfig.FIELD_CONFIG, "value")
    );
  }

  @TestFactory
  public Stream<DynamicTest> apply() {
    List<SchemaAndValue> inputs = Arrays.asList(
        new SchemaAndValue(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE),
        new SchemaAndValue(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE),
        new SchemaAndValue(Schema.INT8_SCHEMA, Byte.MAX_VALUE),
        new SchemaAndValue(Schema.INT16_SCHEMA, Short.MAX_VALUE),
        new SchemaAndValue(Schema.INT32_SCHEMA, Integer.MAX_VALUE),
        new SchemaAndValue(Schema.INT64_SCHEMA, Long.MAX_VALUE),
        new SchemaAndValue(Decimal.schema(2), BigDecimal.valueOf(1234231, 2))
    );
    return inputs.stream().map(i -> dynamicTest(SchemaKey.of(i.schema()).toString(), () -> {

      final Schema valueSchema = SchemaBuilder.struct()
          .name("value")
          .field("name", Schema.STRING_SCHEMA)
          .field("value", i.schema())
          .build();
      final Struct value = new Struct(valueSchema)
          .put("name", "testing")
          .put("value", i.value());

      final SinkRecord input = write(
          "test",
          struct("key",
              "id", Type.INT64, false, 1234L
          ),
          value
      );
      final Struct expectedStruct = struct("value",
          "name", Type.STRING, false, "testing",
          "value", Type.INT64, false, ((Number) i.value()).longValue()
      );

      SinkRecord output = this.transformation.apply(input);
      assertNotNull(output, "output cannot be null.");
      assertNotNull(output.value(), "output.value() cannot be null.");
      assertTrue(output.value() instanceof Struct, "output.value() should be a struct.");
      assertNotNull(output.valueSchema(), "output.valueSchema() cannot be null.");
      assertStruct(expectedStruct, (Struct) output.value());
    }));
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends ToLongTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new ToLong.Value<>();
    }
  }
}
