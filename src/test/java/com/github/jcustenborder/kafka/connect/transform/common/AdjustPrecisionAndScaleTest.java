package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AdjustPrecisionAndScaleTest {
  SinkRecord record(Struct struct) {
    return new SinkRecord("test", 1, null, null, struct.schema(), struct, 1234L);
  }

  @Test
  public void noop() {
    Schema schema = SchemaBuilder.struct()
        .field("first", Schema.STRING_SCHEMA)
        .field("last", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("first", "test")
        .put("last", "user")
        .put("email", "none@none.com");
    SinkRecord record = record(struct);
    AdjustPrecisionAndScale.Value<SinkRecord> transform = new AdjustPrecisionAndScale.Value<>();
    transform.configure(
        ImmutableMap.of(AdjustPrecisionAndScaleConfig.PRECISION_VALUE_CONFIG, 32)
    );
    SinkRecord actual = transform.apply(record);
    assertNotNull(actual);
    assertStruct((Struct) record.value(), (Struct) actual.value());
  }

  @Test
  public void convert() {
    final Schema inputSchema = SchemaBuilder.struct()
        .field("first", Decimal.schema(5))
        .field(
            "second",
            Decimal.builder(5)
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "0")
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_SCALE_PROP, "127")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "48")
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_SCALE_PROP, "16")
                .optional()
                .build()
        )
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("first", BigDecimal.ONE)
        .put("second", null)
        .put("third", BigDecimal.ONE);
    final Schema expectedSchema = SchemaBuilder.struct()
        .field(
            "first",
            Decimal.builder(5)
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "38")
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_SCALE_PROP, "20")
                .build()
        )
        .field(
            "second",
            Decimal.builder(5)
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "38")
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_SCALE_PROP, "20")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "38")
                .parameter(AdjustPrecisionAndScale.CONNECT_AVRO_DECIMAL_SCALE_PROP, "16")
                .optional()
                .build()
        )
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first", new BigDecimal("1.00000000000000000000"))
        .put("second", null)
        .put("third", new BigDecimal("1.0000000000000000"));


    SinkRecord record = record(inputStruct);
    AdjustPrecisionAndScale.Value<SinkRecord> transform = new AdjustPrecisionAndScale.Value<>();
    transform.configure(
        ImmutableMap.of(AdjustPrecisionAndScaleConfig.PRECISION_VALUE_CONFIG , 38,
                        AdjustPrecisionAndScaleConfig.PRECISION_MODE_CONFIG, "max",
                        AdjustPrecisionAndScaleConfig.SCALE_VALUE_CONFIG, 20,
                        AdjustPrecisionAndScaleConfig.SCALE_MODE_CONFIG, "max")
    );


    SinkRecord actual = transform.apply(record);
    assertNotNull(actual);
    assertStruct(expectedStruct, (Struct) actual.value());
  }

}
