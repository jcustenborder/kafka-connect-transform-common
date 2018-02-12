# Introduction

This project provides some common transformation functionality for Kafka Connect.

# Transformations

## ExtractNestedField(Key)

This transformation is used to extract a field from a nested struct and append it to the parent struct.

### Configuration

| Name                   | Type   | Importance | Default Value | Validator | Documentation                                                                                                                                   |
| ---------------------- | ------ | ---------- | ------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------|
| input.inner.field.name | String | High       |               |           | The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.|
| input.outer.field.name | String | High       |               |           | The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.      |
| output.field.name      | String | High       |               |           | The field to place the extracted value into.                                                                                                    |


#### Standalone Example

```properties
transforms=Key
transforms.Key.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key
# The following values must be configured.
transforms.Key.input.inner.field.name=
transforms.Key.input.outer.field.name=
transforms.Key.output.field.name=
```

#### Distributed Example

```json
{
"name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key",
        "transforms": "Key",
        "transforms.Key.type": "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key",
        "transforms.Key.input.inner.field.name":"",
        "transforms.Key.input.outer.field.name":"",
        "transforms.Key.output.field.name":"",
    }
}
```

## ExtractNestedField(Value)

This transformation is used to extract a field from a nested struct and append it to the parent struct.

### Configuration

| Name                   | Type   | Importance | Default Value | Validator | Documentation                                                                                                                                   |
| ---------------------- | ------ | ---------- | ------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------|
| input.inner.field.name | String | High       |               |           | The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.|
| input.outer.field.name | String | High       |               |           | The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.      |
| output.field.name      | String | High       |               |           | The field to place the extracted value into.                                                                                                    |


#### Standalone Example

```properties
transforms=Value
transforms.Value.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value
# The following values must be configured.
transforms.Value.input.inner.field.name=
transforms.Value.input.outer.field.name=
transforms.Value.output.field.name=
```

#### Distributed Example

```json
{
"name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value",
        "transforms": "Value",
        "transforms.Value.type": "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value",
        "transforms.Value.input.inner.field.name":"",
        "transforms.Value.input.outer.field.name":"",
        "transforms.Value.output.field.name":"",
    }
}
```

## ExtractTimestamp(Value)

This transformation is used to use a field from the input data to override the timestamp for the record.

### Configuration

| Name       | Type   | Importance | Default Value | Validator | Documentation                                                              |
| ---------- | ------ | ---------- | ------------- | --------- | ---------------------------------------------------------------------------|
| field.name | String | High       |               |           | The field to pull the timestamp from. This must be an int64 or a timestamp.|


#### Standalone Example

```properties
transforms=Value
transforms.Value.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value
# The following values must be configured.
transforms.Value.field.name=
```

#### Distributed Example

```json
{
"name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value",
        "transforms": "Value",
        "transforms.Value.type": "com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value",
        "transforms.Value.field.name":"",
    }
}
```

## PatternRename(Key)

This transformation is used to rename fields in the key of an input struct based on a regular expression and a replacement string.

### Configuration

| Name                | Type   | Importance | Default Value      | Validator                                                                                                             | Documentation|
| ------------------- | ------ | ---------- | ------------------ | --------------------------------------------------------------------------------------------------------------------- | -------------|
| field.pattern       | String | High       |                    |                                                                                                                       |              |
| field.replacement   | String | High       |                    |                                                                                                                       |              |
| field.pattern.flags | List   | Low        | [CASE_INSENSITIVE] | [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES] |              |


#### Standalone Example

```properties
transforms=Key
transforms.Key.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key
# The following values must be configured.
transforms.Key.field.pattern=
transforms.Key.field.replacement=
```

#### Distributed Example

```json
{
"name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key",
        "transforms": "Key",
        "transforms.Key.type": "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key",
        "transforms.Key.field.pattern":"",
        "transforms.Key.field.replacement":"",
    }
}
```

## PatternRename(Value)

This transformation is used to rename fields in the value of an input struct based on a regular expression and a replacement string.

### Configuration

| Name                | Type   | Importance | Default Value      | Validator                                                                                                             | Documentation|
| ------------------- | ------ | ---------- | ------------------ | --------------------------------------------------------------------------------------------------------------------- | -------------|
| field.pattern       | String | High       |                    |                                                                                                                       |              |
| field.replacement   | String | High       |                    |                                                                                                                       |              |
| field.pattern.flags | List   | Low        | [CASE_INSENSITIVE] | [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES] |              |


#### Standalone Example

```properties
transforms=Value
transforms.Value.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value
# The following values must be configured.
transforms.Value.field.pattern=
transforms.Value.field.replacement=
```

#### Distributed Example

```json
{
"name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value",
        "transforms": "Value",
        "transforms.Value.type": "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value",
        "transforms.Value.field.pattern":"",
        "transforms.Value.field.replacement":"",
    }
}
```

