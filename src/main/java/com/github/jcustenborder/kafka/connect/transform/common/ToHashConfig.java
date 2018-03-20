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
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ToHashConfig extends AbstractConfig {
  public final Schema.Type outputType;
  public final HashAlgorithm hashAlgorithm;
  public final Set<String> fields;
  public final HashFunction hashFunction;

  public static final String OUTPUT_TYPE_CONFIG = "output.outputSchema";
  static final String OUTPUT_TYPE_DOC = "The type of output for the hash.";

  public static final String HASH_ALGORITHM_CONFIG = "hash.algorithm";
  static final String HASH_ALGORITHM_DOC = "The hash algorithm to use when hashing the input value.";

  public static final String FIELD_CONFIG = "fields";
  public static final String FIELD_DOC = "The fields to transform.";

  private final Schema optionalSchema;
  private final Schema schema;

  public enum HashAlgorithm {
    CRC32,
    MD5,
    MURMUR3_32,
    MURMUR3_128,
    SHA1,
    SHA256,
    SHA512
  }

  public ToHashConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.hashAlgorithm = ConfigUtils.getEnum(HashAlgorithm.class, this, HASH_ALGORITHM_CONFIG);
    this.outputType = ConfigUtils.getEnum(Schema.Type.class, this, OUTPUT_TYPE_CONFIG);
    List<String> fields = getList(FIELD_CONFIG);
    this.fields = new HashSet<>(fields);
    switch (this.hashAlgorithm) {
      case MD5:
        this.hashFunction = Hashing.md5();
        break;
      case CRC32:
        this.hashFunction = Hashing.crc32();
        break;
      case MURMUR3_32:
        this.hashFunction = Hashing.murmur3_32();
        break;
      case MURMUR3_128:
        this.hashFunction = Hashing.murmur3_128();
        break;
      case SHA1:
        this.hashFunction = Hashing.sha1();
        break;
      case SHA256:
        this.hashFunction = Hashing.sha256();
        break;
      case SHA512:
        this.hashFunction = Hashing.sha512();
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("%s is an unsupported hash algorithm.", this.hashAlgorithm)
        );
    }

    switch (this.outputType) {
      case BYTES:
        this.optionalSchema = Schema.OPTIONAL_BYTES_SCHEMA;
        this.schema = Schema.BYTES_SCHEMA;
        break;
      case STRING:
        this.optionalSchema = Schema.OPTIONAL_STRING_SCHEMA;
        this.schema = Schema.STRING_SCHEMA;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                ToHashConfig.OUTPUT_TYPE_CONFIG + " of '%s' is not supported.",
                this.outputType
            )
        );
    }

  }

  Schema outputSchema(boolean isOptional) {
    return isOptional ? optionalSchema : schema;
  }


  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(OUTPUT_TYPE_CONFIG, ConfigDef.Type.STRING)
                .documentation(OUTPUT_TYPE_DOC)
                .defaultValue(Schema.Type.STRING.toString())
                .validator(ConfigDef.ValidString.in(
                    Schema.Type.STRING.toString(),
                    Schema.Type.BYTES.toString()
                ))
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(HASH_ALGORITHM_CONFIG, ConfigDef.Type.STRING)
                .documentation(HASH_ALGORITHM_DOC)
                .validator(ValidEnum.of(HashAlgorithm.class))
                .defaultValue(HashAlgorithm.SHA1.toString())
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(FIELD_CONFIG, ConfigDef.Type.LIST)
                .documentation(FIELD_DOC)
                .defaultValue(Collections.emptyList())
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        );
  }

}
