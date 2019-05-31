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

public class SetMaximumPrecisionTest {
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
        .put("first", "none@none.com");
    SinkRecord record = record(struct);
    SetMaximumPrecision.Value<SinkRecord> transform = new SetMaximumPrecision.Value<>();
    transform.configure(
        ImmutableMap.of(SetMaximumPrecisionConfig.MAX_PRECISION_CONFIG, 32)
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
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "16")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "48")
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
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "32")
                .build()
        )
        .field(
            "second",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "16")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "32")
                .optional()
                .build()
        )
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first", BigDecimal.ONE)
        .put("second", null)
        .put("third", BigDecimal.ONE);


    SinkRecord record = record(inputStruct);
    SetMaximumPrecision.Value<SinkRecord> transform = new SetMaximumPrecision.Value<>();
    transform.configure(
        ImmutableMap.of(SetMaximumPrecisionConfig.MAX_PRECISION_CONFIG, 32)
    );


    SinkRecord actual = transform.apply(record);
    assertNotNull(actual);
    assertStruct(expectedStruct, (Struct) actual.value());
  }

}
