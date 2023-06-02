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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimestampNowFieldTest {

  TimestampNowField<SinkRecord> transformation;
  static Instant timestampNow = Instant.parse("2023-06-02T10:06:45+00:00");
  static Date timestampNowAsDate = Date.from(timestampNow);

  @BeforeEach
  public void beforeEach() {
    this.transformation = new TimestampNowField.Value<>();
    Time time = mock(Time.class);
    when(time.milliseconds()).thenReturn(timestampNow.toEpochMilli());
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
    DATE(TimestampNowFieldTargetType.DATE, Timestamp.SCHEMA, timestampNowAsDate),
    UNIX(TimestampNowFieldTargetType.UNIX, Schema.INT64_SCHEMA, timestampNow.getEpochSecond());

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
  public void structAddTwoDays() {
    Date timestampNowPlusTwoDays = Date.from(timestampNow.plus(2, ChronoUnit.DAYS));
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.ADD_AMOUNT_CONF, "2",
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
            .put("timestamp", timestampNowPlusTwoDays);
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
        "firstName", "example", "lastName", "user", "timestamp", timestampNowAsDate
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
  public void mapAddThreeHours() {
    Date timestampPlusThreeHours = Date.from(timestampNow.plus(3, ChronoUnit.HOURS));
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.ADD_AMOUNT_CONF, "3",
                    TimestampNowFieldConfig.ADD_CHRONO_UNIT_CONF, "HOURS"
            )
    );
    final Map<String, Object> expected = ImmutableMap.of(
            "firstName", "example", "lastName", "user", "timestamp", timestampPlusThreeHours
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
    long timestampFormattedAsUnix = timestampNow.getEpochSecond();
    this.transformation.configure(
            ImmutableMap.of(
                    TimestampNowFieldConfig.FIELDS_CONF, "timestamp",
                    TimestampNowFieldConfig.TARGET_TYPE_CONF, "UNIX"
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
