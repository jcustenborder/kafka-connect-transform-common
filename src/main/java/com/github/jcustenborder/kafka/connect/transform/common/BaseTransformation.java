package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public abstract class BaseTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  protected abstract SchemaAndValue processStruct(R record, SchemaAndValue schemaAndValue);

  protected abstract SchemaAndValue processMap(R record, SchemaAndValue schemaAndValue);

  protected SchemaAndValue process(R record, SchemaAndValue schemaAndValue) {
    final SchemaAndValue result;
    if (schemaAndValue.value() instanceof Struct) {
      result = processStruct(record, schemaAndValue);
    } else if (schemaAndValue.value() instanceof Map) {
      result = processMap(record, schemaAndValue);
    } else {
      throw new UnsupportedOperationException();
    }
    return result;
  }
}
