package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimestampNowFieldTest {

  TimestampNowField<SinkRecord> transformation;
  static Date timestamp = new Date(1586963336123L);

  @BeforeEach
  public void beforeEach() {
    this.transformation = new TimestampNowField.Value<>();
    Time time = mock(Time.class);
    when(time.milliseconds()).thenReturn(timestamp.getTime());
    this.transformation.time = time;
    this.transformation.configure(
        ImmutableMap.of(TimestampNowFieldConfig.FIELDS_CONF, "timestamp")
    );
  }

  @BeforeEach
  public void afterEach() {
    this.transformation.close();
  }

  enum StructUseCases {
    Date(TimestampNowFieldTargetType.DATE, Timestamp.SCHEMA, timestamp),
    Unix(TimestampNowFieldTargetType.UNIX, Schema.INT64_SCHEMA, timestamp.toInstant().getEpochSecond());

    public final TimestampNowFieldTargetType targetType;
    public final Schema schema;
    public final Object value;


    StructUseCases(TimestampNowFieldTargetType targetType, Schema schema, Object value) {
      this.targetType = targetType;
      this.schema = schema;
      this.value = value;
    }
  }

  @ParameterizedTest
  @EnumSource(StructUseCases.class)
  public void structFieldMissing(StructUseCases useCase) {
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.TARGET_TYPE_CONF, useCase.targetType.name()
            )
    );
    final Schema inputSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("timestamp", useCase.schema)
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("firstName", "example")
        .put("lastName", "user");
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("firstName", "example")
        .put("lastName", "user")
        .put("timestamp", useCase.value);
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Struct, "value should be a struct");
    final Struct actualStruct = (Struct) output.value();
    assertStruct(expectedStruct, actualStruct);
  }

  @ParameterizedTest
  @EnumSource(StructUseCases.class)
  public void structFieldExists(StructUseCases useCase) {
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.TARGET_TYPE_CONF, useCase.targetType.name()
            )
    );
    final Schema inputSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("timestamp", useCase.schema)
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("firstName", "example")
        .put("lastName", "user");
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("firstName", "example")
        .put("lastName", "user")
        .put("timestamp", useCase.value);
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Struct, "value should be a struct");
    final Struct actualStruct = (Struct) output.value();
    assertStruct(expectedStruct, actualStruct);
  }

  @Test
  public void structAddOneDay() {
    Date timestampPlusOneDay = Date.from(timestamp.toInstant().plus(1, ChronoUnit.DAYS));
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.ADD_AMOUNT_CONF, "1",
                    TimestampNowFieldConfig.ADD_CHRONO_UNIT_CONF, "DAYS"
            )
    );
    final Schema inputSchema = SchemaBuilder.struct()
            .name("something")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .build();
    final Schema expectedSchema = SchemaBuilder.struct()
            .name("something")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .build();
    final Struct inputStruct = new Struct(inputSchema)
            .put("firstName", "example")
            .put("lastName", "user");
    final Struct expectedStruct = new Struct(expectedSchema)
            .put("firstName", "example")
            .put("lastName", "user")
            .put("timestamp", timestampPlusOneDay);
    final SinkRecord input = new SinkRecord(
            "test",
            1,
            null,
            null,
            inputSchema,
            inputStruct,
            1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Struct, "value should be a struct");
    final Struct actualStruct = (Struct) output.value();
    assertStruct(expectedStruct, actualStruct);
  }

  @ParameterizedTest
  @EnumSource(StructUseCases.class)
  public void structFieldMismatch(StructUseCases useCase) {
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.TARGET_TYPE_CONF, useCase.targetType.name()
            )
    );
    final Schema inputSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("timestamp", Schema.STRING_SCHEMA)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .name("something")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("timestamp", useCase.schema)
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("firstName", "example")
        .put("lastName", "user");
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("firstName", "example")
        .put("lastName", "user")
        .put("timestamp", useCase.value);
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Struct, "value should be a struct");
    final Struct actualStruct = (Struct) output.value();
    assertStruct(expectedStruct, actualStruct);
  }

  @Test
  public void mapFieldMissing() {
    final Map<String, Object> expected = ImmutableMap.of(
        "firstName", "example", "lastName", "user", "timestamp", timestamp
    );
    final SinkRecord input = new SinkRecord(
        "test",
        1,
        null,
        null,
        null,
        ImmutableMap.of("firstName", "example", "lastName", "user"),
        1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Map, "value should be a struct");
    final Map<String, Object> actual = (Map<String, Object>) output.value();
    assertEquals(expected, actual);
  }

  @Test
  public void mapAddOneDay() {
    Date timestampPlusOneDay = Date.from(timestamp.toInstant().plus(1, ChronoUnit.DAYS));
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.ADD_AMOUNT_CONF, "1",
                    TimestampNowFieldConfig.ADD_CHRONO_UNIT_CONF, "DAYS"
            )
    );
    final Map<String, Object> expected = ImmutableMap.of(
            "firstName", "example", "lastName", "user", "timestamp", timestampPlusOneDay
    );
    final SinkRecord input = new SinkRecord(
            "test",
            1,
            null,
            null,
            null,
            ImmutableMap.of("firstName", "example", "lastName", "user"),
            1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Map, "value should be a struct");
    final Map<String, Object> actual = (Map<String, Object>) output.value();
    assertEquals(expected, actual);
  }

  @Test
  public void mapFormattedAsUnix() {
    long timestampFormattedAsUnix = timestamp.toInstant().getEpochSecond();
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.TARGET_TYPE_CONF, "Unix"
            )
    );
    final Map<String, Object> expected = ImmutableMap.of(
            "firstName", "example", "lastName", "user", "timestamp", timestampFormattedAsUnix
    );
    final SinkRecord input = new SinkRecord(
            "test",
            1,
            null,
            null,
            null,
            ImmutableMap.of("firstName", "example", "lastName", "user"),
            1234L
    );
    final SinkRecord output = this.transformation.apply(input);
    assertNotNull(output, "output should not be null.");
    assertTrue(output.value() instanceof Map, "value should be a struct");
    final Map<String, Object> actual = (Map<String, Object>) output.value();
    assertEquals(expected, actual);
  }

  @Test
  public void config() {
    assertNotNull(this.transformation.config());
  }

}
