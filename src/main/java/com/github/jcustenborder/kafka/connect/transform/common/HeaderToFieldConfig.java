/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

class HeaderToFieldConfig extends AbstractConfig {
  private static final Logger log = LoggerFactory.getLogger(HeaderToFieldConfig.class);

  public static final String HEADER_MAPPINGS_CONF = "header.mappings";
  static final String HEADER_MAPPINGS_DOC = "The mapping of the header to the field in the message.";

  public final List<HeaderToFieldMapping> mappings;

  static class HeaderToFieldMapping {
    final String header;
    final Schema schema;
    final String field;

    HeaderToFieldMapping(String header, Schema schema, String field) {
      this.header = header;
      this.schema = schema;
      this.field = field;
    }

    private static final Map<String, Schema> SCHEMA_TYPE_LOOKUP;

    static final void addSchema(Map<String, Schema> schemaTypeLookup, Schema schema) {
      addSchema(schemaTypeLookup, schema, null);
    }

    static final void addSchema(Map<String, Schema> schemaTypeLookup, Schema schema, String shortName) {
      Preconditions.checkState(schema.isOptional(), "Schema must be optional: %s", schema);
      String schemaKey = toString(schema, shortName);
      schemaTypeLookup.put(schemaKey, schema);
    }

    static {
      Map<String, Schema> schemaTypeLookup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

      Set<Schema.Type> skip = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT);
      Arrays.stream(Schema.Type.values())
          .filter(type -> !skip.contains(type))
          .map(e -> SchemaBuilder.type(e).optional().build())
          .forEach(s -> {
            addSchema(schemaTypeLookup, s);
          });
      List<Schema> logicalTypes = new ArrayList<>(100);
      IntStream.range(0, 50)
          .mapToObj(value -> Decimal.builder(value).optional().build())
          .forEach(logicalTypes::add);
      logicalTypes.add(Timestamp.builder().optional().build());
      logicalTypes.add(Date.builder().optional().build());
      logicalTypes.add(Time.builder().optional().build());
      logicalTypes.add(Decimal.builder(1).optional().build());

      logicalTypes
          .forEach(schema -> {
            String shortName = schema.name().replaceAll("^.+\\.(.+)$", "$1");
            addSchema(schemaTypeLookup, schema);
            addSchema(schemaTypeLookup, schema, shortName);
          });


      SCHEMA_TYPE_LOOKUP = schemaTypeLookup;
    }


    public static HeaderToFieldMapping parse(String input) {
      log.trace("parse() - input = '{}'", input);
      Preconditions.checkNotNull(input, "input cannot be null");
      String[] parts = input.split(":");
      Preconditions.checkState(parts.length >= 2 && parts.length <= 3, "input must be in <header>:<schema>[:<field>] format.");
      final String headerName = parts[0].trim();
      final String fieldName = parts.length == 3 ? parts[2] : headerName;
      final String schemaText = parts[1].trim();
      final Schema schema;

      if (SCHEMA_TYPE_LOOKUP.containsKey(schemaText)) {
        schema = SCHEMA_TYPE_LOOKUP.get(schemaText);
      } else {
        throw new UnsupportedOperationException(
            "Could not find schema for " + schemaText
        );
      }

      return new HeaderToFieldMapping(headerName, schema, fieldName);
    }

    public static String toString(Schema schema) {
      return toString(schema, null);
    }

    public static String toString(Schema schema, String schemaNameOverride) {
      StringBuilder builder = new StringBuilder();
      builder.append(schema.type());
      if (!Strings.isNullOrEmpty(schema.name())) {
        String schemaName = Strings.isNullOrEmpty(schemaNameOverride) ? schema.name() : schemaNameOverride;
        builder.append("(");
        builder.append(schemaName);

        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
          builder.append("[scale=");
          builder.append(schema.parameters().get(Decimal.SCALE_FIELD));
          builder.append("]");
        }

        builder.append(")");
      }
      return builder.toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.header, this.field, this.schema);
    }

    @Override
    public boolean equals(Object obj) {
      boolean result;
      if ((obj instanceof HeaderToFieldMapping)) {
        HeaderToFieldMapping that = (HeaderToFieldMapping) obj;
        result = this.field.equals(that.field) &&
            this.schema.equals(that.schema) &&
            this.header.equals(that.header);
      } else {
        result = false;
      }
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("header", this.header)
          .add("field", this.field)
          .add("schema", this.schema)
          .toString();
    }
  }


  public HeaderToFieldConfig(Map<?, ?> originals) {
    super(config(), originals);

    List<HeaderToFieldMapping> mappings = new ArrayList<>();
    List<String> rawMappings = getList(HEADER_MAPPINGS_CONF);

    for (String rawMapping : rawMappings) {
      try {
        HeaderToFieldMapping mapping = HeaderToFieldMapping.parse(rawMapping);
        mappings.add(mapping);
      } catch (Exception ex) {
        ConfigException configException = new ConfigException(HEADER_MAPPINGS_CONF, rawMapping);
        configException.initCause(ex);
        throw configException;
      }
    }

    this.mappings = ImmutableList.copyOf(mappings);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(HEADER_MAPPINGS_CONF, ConfigDef.Type.LIST)
                .documentation(HEADER_MAPPINGS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        );
  }
}
