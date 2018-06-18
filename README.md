
# Introduction

This project contains common transformations for every day use cases with Kafka Connect.

Table of Contents
=================

  * [BytesToString$Key](#bytestostringkey)
  * [BytesToString$Value](#bytestostringvalue)
  * [ChangeCase$Key](#changecasekey)
  * [ChangeCase$Value](#changecasevalue)
  * [ChangeTopicCase](#changetopiccase)
  * [ExtractNestedField$Key](#extractnestedfieldkey)
  * [ExtractNestedField$Value](#extractnestedfieldvalue)
  * [ExtractTimestamp$Value](#extracttimestampvalue)
  * [PatternRename$Key](#patternrenamekey)
  * [PatternRename$Value](#patternrenamevalue)
  * [ToJson$Key](#tojsonkey)
  * [ToJson$Value](#tojsonvalue)
  


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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Key
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Key"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Value
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Value"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



##### `to`



*Importance:* High

*Type:* String

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Key
transforms.tran.from=< Required Configuration >
transforms.tran.to=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Key",
  "transforms.tran.from" : "< Required Configuration >",
  "transforms.tran.to" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



## ChangeCase(Value)

This transformation is used to change the case of fields in an input struct.



### Configuration

#### General


##### `from`

The format to move from 

*Importance:* High

*Type:* String

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



##### `to`



*Importance:* High

*Type:* String

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Value
transforms.tran.from=< Required Configuration >
transforms.tran.to=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Value",
  "transforms.tran.from" : "< Required Configuration >",
  "transforms.tran.to" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



##### `to`

The format of the outgoing topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* High

*Type:* String

*Validator:* ValidEnum{enum=CaseFormat, allowed=[LOWER_HYPHEN, LOWER_UNDERSCORE, LOWER_CAMEL, UPPER_CAMEL, UPPER_UNDERSCORE]}



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ChangeTopicCase
transforms.tran.from=< Required Configuration >
transforms.tran.to=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ChangeTopicCase",
  "transforms.tran.from" : "< Required Configuration >",
  "transforms.tran.to" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key
transforms.tran.input.inner.field.name=< Required Configuration >
transforms.tran.input.outer.field.name=< Required Configuration >
transforms.tran.output.field.name=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key",
  "transforms.tran.input.inner.field.name" : "< Required Configuration >",
  "transforms.tran.input.outer.field.name" : "< Required Configuration >",
  "transforms.tran.output.field.name" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value
transforms.tran.input.inner.field.name=< Required Configuration >
transforms.tran.input.outer.field.name=< Required Configuration >
transforms.tran.output.field.name=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value",
  "transforms.tran.input.inner.field.name" : "< Required Configuration >",
  "transforms.tran.input.outer.field.name" : "< Required Configuration >",
  "transforms.tran.output.field.name" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



## ExtractTimestamp(Value)

This transformation is used to use a field from the input data to override the timestamp for the record.



### Configuration

#### General


##### `field.name`

The field to pull the timestamp from. This must be an int64 or a timestamp.

*Importance:* High

*Type:* String



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value
transforms.tran.field.name=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value",
  "transforms.tran.field.name" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key
transforms.tran.field.pattern=< Required Configuration >
transforms.tran.field.replacement=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key",
  "transforms.tran.field.pattern" : "< Required Configuration >",
  "transforms.tran.field.replacement" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value
transforms.tran.field.pattern=< Required Configuration >
transforms.tran.field.replacement=< Required Configuration >
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value",
  "transforms.tran.field.pattern" : "< Required Configuration >",
  "transforms.tran.field.replacement" : "< Required Configuration >"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Key
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Key"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```



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



#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Value
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Value"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```


