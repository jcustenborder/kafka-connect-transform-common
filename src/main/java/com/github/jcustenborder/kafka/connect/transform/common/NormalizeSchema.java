/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Title("NormalizeSchema")
@Description("This transformation is used to convert older schema versions to the latest schema version. This works by " +
    "keying all of the schemas that are coming into the transformation by their schema name and comparing the version() of " +
    "the schema. The latest version of a schema will be used. Schemas are discovered as the flow through the transformation. " +
    "The latest version of a schema is what is used.")
public abstract class NormalizeSchema<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(NormalizeSchema.class);

  /**
   * A loose schema key is required. In this case we only want to match on the type and name. The
   * version will be used to determine the latest schema version later.
   */
  static class SchemaKey implements Comparable<SchemaKey> {
    final String schemaName;
    final Schema.Type type;

    private SchemaKey(String schemaName, Schema.Type type) {
      this.schemaName = schemaName;
      this.type = type;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.type, this.schemaName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("type", this.type)
          .add("schemaName", this.schemaName)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SchemaKey) {
        SchemaKey that = (SchemaKey) o;
        return 0 == this.compareTo(that);
      } else {
        return false;
      }
    }

    @Override
    public int compareTo(SchemaKey that) {
      if (null == that) {
        return -1;
      }

      return ComparisonChain.start()
          .compare(this.type, that.type)
          .compare(this.schemaName, that.schemaName)
          .result();
    }

    public static SchemaKey of(Schema schema) {
      return new SchemaKey(schema.name(), schema.type());
    }
  }

  static class SchemaState {
    Integer latestVersion = null;
    final Map<Integer, Schema> schemaVersions = new HashMap<>();

    public SchemaState(Schema schema) {
      addSchema(schema);
    }

    public boolean hasSchema(Schema schema) {
      return this.schemaVersions.containsKey(schema.version());
    }

    public void addSchema(Schema schema) {
      this.schemaVersions.put(schema.version(), schema);
      Optional<Integer> newLatestVersion = this.schemaVersions.keySet().stream()
          .max(Integer::compareTo);
      if (!newLatestVersion.isPresent()) {
        throw new DataException("Could not determine latest schema.");
      }
      log.trace(
          "addSchema() - Adding version {}. latestVersion = {} newLatestVersion = {}",
          schema.version(),
          this.latestVersion,
          newLatestVersion.get()
      );
      this.latestVersion = newLatestVersion.get();
    }

    public boolean shouldConvert(Schema schema) {
      return !this.latestVersion.equals(schema.version());
    }

    public Schema latest() {
      return this.schemaVersions.get(this.latestVersion);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("latestVersion", this.latestVersion)
          .add("schemaVersions", this.schemaVersions)
          .toString();
    }
  }


  protected SchemaAndValue normalize(R r, Schema schema, Object value) {
    return normalize(new SchemaAndValue(schema, value));
  }

  Map<SchemaKey, SchemaState> stateLookup = new HashMap<>();

  void copy(Struct input, Struct output) {
    for (Field outputField : output.schema().fields()) {
      Field inputField = input.schema().field(outputField.name());
      if (null != inputField) {
        if (Schema.Type.STRUCT == outputField.schema().type()) {
          Struct inputStruct = input.getStruct(inputField.name());
          if (null == inputStruct) {
            output.put(outputField, null);
          } else {
            Struct outputStruct = new Struct(outputField.schema());
            copy(inputStruct, outputStruct);
          }
        } else {
          output.put(outputField, input.get(outputField.name()));
        }
      } else {
        log.trace("copy() - Skipping '{}' because input does not have field.", outputField.name());
      }
    }
  }

  protected SchemaAndValue normalize(SchemaAndValue input) {
    if (null == input.value() || null == input.schema()) {
      log.trace("normalize() - input.value() or input.schema() is null.");
      return input;
    }
    final Schema inputSchema = input.schema();

    if (Schema.Type.STRUCT != inputSchema.type()) {
      log.trace("normalize() - inputSchema.type('{}') is not a struct.", inputSchema.type());
      return input;
    }

    SchemaState state = stateLookup.computeIfAbsent(SchemaKey.of(inputSchema), schemaKey -> new SchemaState(inputSchema));
    if (!state.hasSchema(inputSchema)) {
      state.addSchema(inputSchema);
    }
    if (!state.shouldConvert(inputSchema)) {
      log.trace(
          "normalize() - Schema is correct version don't need to convert. version = {}",
          inputSchema.version()
      );
      return input;
    }
    Schema latestSchema = state.latest();
    log.trace(
        "normalize() - Converting version {} to {}. schema = '{}', state='{}'",
        inputSchema.version(),
        latestSchema.version(),
        inputSchema,
        state
    );

    Struct inputStruct = (Struct) input.value();

    Struct outputStruct = new Struct(latestSchema);
    copy(inputStruct, outputStruct);
    return new SchemaAndValue(latestSchema, outputStruct);
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }

  public static class Key<R extends ConnectRecord<R>> extends NormalizeSchema<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = normalize(r, r.keySchema(), r.key());

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


  public static class Value<R extends ConnectRecord<R>> extends NormalizeSchema<R> {
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = normalize(r, r.valueSchema(), r.value());

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
