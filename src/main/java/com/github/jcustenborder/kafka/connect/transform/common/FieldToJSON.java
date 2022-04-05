package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class FieldToJSON<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(FieldToJSON.class);

    FieldToJSONConfig config;
    JsonConverter converter = new JsonConverter();
    Map<Schema, Schema> schemaCache;
    List<String> fieldNames;

    @Override
    public ConfigDef config() {
        return FieldToJSONConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> settings) {
        this.config = new FieldToJSONConfig(settings);
        Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
        settingsClone.put(FieldToJSONConfig.SCHEMAS_ENABLE_CONFIG, this.config.schemasEnable);
        this.converter.configure(settingsClone, false);
        this.schemaCache = new HashMap<>();

        this.fieldNames = Arrays.stream(config.fieldName.split(","))
                .map(String::trim)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
        return toJson(inputSchema, input);
    }

    private SchemaAndValue toJson(Schema inputSchema, Struct inputStruct) {

        //map of fieldName -> jsonValue (String or byte[])
        Map<String, Object> jsonValues = transformFieldValuesToJson(inputSchema, inputStruct);

        final Schema outputSchema = schemaCache.computeIfAbsent(inputSchema, key -> buildNewSchema(inputSchema));

        final Struct outputStruct = new Struct(outputSchema);

        for (Field inputField : inputSchema.fields()) {
            if (fieldNames.contains(inputField.name())) {
                outputStruct.put(inputField.name(), jsonValues.get(inputField.name()));
            } else {
                outputStruct.put(inputField.name(), inputStruct.get(inputField));
            }
        }

        return new SchemaAndValue(outputSchema, outputStruct);
    }

    private Map<String, Object> transformFieldValuesToJson(Schema inputSchema, Struct inputStruct) {
        Map<String, Object> jsonValues = new HashMap<>();

        for (String fieldName : fieldNames) {
            Field originalInputField = inputSchema.field(fieldName);

            if (originalInputField == null) {
                throw new DataException(String.format("Schema does not have field '%s'", fieldName));
            }

            Object originalValue = inputStruct.get(originalInputField);

            final byte[] buffer = this.converter.fromConnectData("dummy", originalInputField.schema(), originalValue);
            jsonValues.put(fieldName, getJsonValue(buffer));
        }

        return jsonValues;
    }

    private Object getJsonValue(byte[] buffer) {
        switch (this.config.outputSchema) {
            case STRING:
                return new String(buffer, Charsets.UTF_8);
            case BYTES:
                return buffer;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Schema type (%s)'%s' is not supported.",
                                ToJSONConfig.OUTPUT_SCHEMA_CONFIG,
                                this.config.outputSchema
                        )
                );
        }
    }

    private Schema buildNewSchema(Schema inputSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();

        if (!Strings.isNullOrEmpty(inputSchema.name())) {
            builder.name(inputSchema.name());
        }

        if (inputSchema.isOptional()) {
            builder.optional();
        }

        for (Field inputField : inputSchema.fields()) {
            if (fieldNames.contains(inputField.name())) {
                SchemaBuilder fieldSchema = new SchemaBuilder(config.outputSchema)
                        .name(inputField.name());
                if (inputField.schema().isOptional()) {
                    fieldSchema.isOptional();
                }
                builder.field(inputField.name(), fieldSchema.build());
            } else{
                builder.field(inputField.name(), inputField.schema());
            }
        }

        Schema newSchema = builder.build();

        log.info("[FieldToJSON] New schema constructed: {}", newSchema.fields());

        return newSchema;
    }

    @Title("FieldToJson(Key)")
    @Description("This transformation is used to take a field of a structured data such as AVRO and output it as " +
            "JSON by way of the JsonConverter built into Kafka Connect.")
    @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
    public static class Key<R extends ConnectRecord<R>> extends FieldToJSON<R> {

        @Override
        public R apply(R record) {
            final SchemaAndValue transformed = process(record, record.keySchema(), record.key());

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    transformed.schema(),
                    transformed.value(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }
    }

    @Title("FieldToJson(Value)")
    @Description("This transformation is used to take a field of a structured data such as AVRO and output it as " +
            "JSON by way of the JsonConverter built into Kafka Connect.")
    public static class Value<R extends ConnectRecord<R>> extends FieldToJSON<R> {

        @Override
        public R apply(R record) {
            final SchemaAndValue transformed = process(record, record.valueSchema(), record.value());

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    transformed.schema(),
                    transformed.value(),
                    record.timestamp()
            );
        }
    }
}
