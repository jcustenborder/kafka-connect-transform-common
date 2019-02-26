package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

public abstract class TopicNameToField<R extends ConnectRecord<R>> extends BaseTransformation<R> {

  @Override
  public ConfigDef config() {
    return TopicNameToFieldConfig.config();
  }

  @Override
  public void close() {

  }

  TopicNameToFieldConfig config;
  Schema schema;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new TopicNameToFieldConfig(settings);
    this.schema = SchemaBuilder.string().doc("Topic name");
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    input.put(this.config.field, record.topic());
    return new SchemaAndValue(null, input);
  }

  Map<Schema, Schema> schemaLookup = new HashMap<>();

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final Schema schema = this.schemaLookup.computeIfAbsent(inputSchema, s -> {
      SchemaBuilder builder = SchemaBuilder.struct()
          .name(s.name())
          .doc(s.doc())
          .parameters(s.parameters())
          .version(s.version())
          .defaultValue(s.defaultValue());
      if (inputSchema.isOptional()) {
        builder.optional();
      }
      for (Field field : inputSchema.fields()) {
        builder.field(field.name(), field.schema());
      }
      builder.field(config.field, s);
      return builder.build();
    });
    Struct struct = new Struct(schema);
    for (Field field : input.schema().fields()) {
      Object value = input.get(field.name());
      struct.put(field.name(), value);
    }
    struct.put(this.config.field, record.topic());
    return new SchemaAndValue(schema, struct);
  }

  @Title("TopicNameToField(Key)")
  @Description("This transformation is used to add the topic as a field.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends TopicNameToField<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("TopicNameToField(Value)")
  @Description("This transformation is used to add the topic as a field.")
  public static class Value<R extends ConnectRecord<R>> extends TopicNameToField<R> {
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }

}
