package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class ExtractTimestampConfig extends AbstractConfig {
  public final String fieldName;

  public static final String FIELD_NAME_CONFIG = "field.name";
  public static final String FIELD_NAME_DOC = "";

  public ExtractTimestampConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.fieldName = getString(FIELD_NAME_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FIELD_NAME_DOC);
  }
}
