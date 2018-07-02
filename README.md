
# Introduction


This project contains common transformations for every day use cases with Kafka Connect.




# Transformations


## BytesToString(Key)

This transformation is used to convert a byte array to a string.

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* High

*Type:* String

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* High

*Type:* List

*Default Value:* []





## BytesToString(Value)

This transformation is used to convert a byte array to a string.



### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* High

*Type:* String

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* High

*Type:* List

*Default Value:* []





## ChangeCase(Key)

This transformation is used to change the case of fields in an input struct.

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `from`

The format to move from 

*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`



*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``





## ChangeCase(Value)

This transformation is used to change the case of fields in an input struct.



### Configuration

#### General


##### `from`

The format to move from 

*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`



*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``





## ChangeTopicCase

This transformation is used to change the case of a topic.

### Tip

This transformation will convert a topic name like 'TOPIC_NAME' to `topicName`, or `topic_name`.


### Configuration

#### General


##### `from`

The format of the incoming topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`

The format of the outgoing topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* High

*Type:* String

*Validator:* ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``





## ExtractNestedField(Key)

This transformation is used to extract a field from a nested struct and append it to the parent struct.

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `input.inner.field.name`

The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.

*Importance:* High

*Type:* String



##### `input.outer.field.name`

The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.

*Importance:* High

*Type:* String



##### `output.field.name`

The field to place the extracted value into.

*Importance:* High

*Type:* String





## ExtractNestedField(Value)

This transformation is used to extract a field from a nested struct and append it to the parent struct.



### Configuration

#### General


##### `input.inner.field.name`

The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.

*Importance:* High

*Type:* String



##### `input.outer.field.name`

The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.

*Importance:* High

*Type:* String



##### `output.field.name`

The field to place the extracted value into.

*Importance:* High

*Type:* String





## ExtractTimestamp(Value)

This transformation is used to use a field from the input data to override the timestamp for the record.



### Configuration

#### General


##### `field.name`

The field to pull the timestamp from. This must be an int64 or a timestamp.

*Importance:* High

*Type:* String





## PatternRename(Key)

This transformation is used to rename fields in the key of an input struct based on a regular expression and a replacement string.

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `field.pattern`



*Importance:* High

*Type:* String



##### `field.replacement`



*Importance:* High

*Type:* String



##### `field.pattern.flags`



*Importance:* Low

*Type:* List

*Default Value:* [CASE_INSENSITIVE]

*Validator:* [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES]





## PatternRename(Value)

This transformation is used to rename fields in the value of an input struct based on a regular expression and a replacement string.



### Configuration

#### General


##### `field.pattern`



*Importance:* High

*Type:* String



##### `field.replacement`



*Importance:* High

*Type:* String



##### `field.pattern.flags`



*Importance:* Low

*Type:* List

*Default Value:* [CASE_INSENSITIVE]

*Validator:* [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES]





## ToJson(Key)

This transformation is used to take structured data such as AVRO and output it as JSON by way of the JsonConverter built into Kafka Connect.

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `output.schema.type`

The connect schema type to output the converted JSON as.

*Importance:* Medium

*Type:* String

*Default Value:* STRING

*Validator:* [STRING, BYTES]



##### `schemas.enable`

Flag to determine if the JSON data should include the schema.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false





## ToJson(Value)

This transformation is used to take structured data such as AVRO and output it as JSON by way of the JsonConverter built into Kafka Connect.



### Configuration

#### General


##### `output.schema.type`

The connect schema type to output the converted JSON as.

*Importance:* Medium

*Type:* String

*Default Value:* STRING

*Validator:* [STRING, BYTES]



##### `schemas.enable`

Flag to determine if the JSON data should include the schema.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false





## ToLong(Key)

This transformation is used to convert a number to a long

### Tip

This transformation is used to manipulate fields in the Key of the record.


### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* High

*Type:* String

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* High

*Type:* List

*Default Value:* []





## ToLong(Value)

This transformation is used to convert a number to a long



### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* High

*Type:* String

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* High

*Type:* List

*Default Value:* []





# Development

## Building the source

```bash
mvn clean package
```

