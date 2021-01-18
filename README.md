# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common) | [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-transform-common)


This project contains common transformations for every day use cases with Kafka Connect.

# Installation

## Confluent Hub

The following command can be used to install the plugin directly from the Confluent Hub using the
[Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

```bash
confluent-hub install jcustenborder/kafka-connect-transform-common:latest
```

## Manually

The zip file that is deployed to the [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-transform-common) is available under
`target/components/packages/`. You can manually extract this zip file which includes all dependencies. All the dependencies
that are required to deploy the plugin are under `target/kafka-connect-target` as well. Make sure that you include all the dependencies that are required
to run the plugin.

1. Create a directory under the `plugin.path` on your Connect worker.
2. Copy all of the dependencies under the newly created subdirectory.
3. Restart the Connect worker.




# Transformations
## [BytesToString](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/BytesToString.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.BytesToString$Value
```


### Configuration

#### General


##### `charset`

The charset to use when creating the output string.

*Importance:* HIGH

*Type:* STRING

*Default Value:* UTF-8



##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [ChangeCase](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ChangeCase.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ChangeCase$Value
```


### Configuration

#### General


##### `from`

The format to move from 

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`



*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``




## [ChangeTopicCase](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ChangeTopicCase.html)

```
com.github.jcustenborder.kafka.connect.transform.common.ChangeTopicCase
```

This transformation is used to change the case of a topic.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_change_the_topic_case) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=274s)

### Tip

This transformation will convert a topic name like 'TOPIC_NAME' to `topicName`, or `topic_name`.
### Configuration

#### General


##### `from`

The format of the incoming topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``



##### `to`

The format of the outgoing topic name. `LOWER_CAMEL` = Java variable naming convention, e.g., "lowerCamel". `LOWER_HYPHEN` = Hyphenated variable naming convention, e.g., "lower-hyphen". `LOWER_UNDERSCORE` = C++ variable naming convention, e.g., "lower_underscore". `UPPER_CAMEL` = Java and C++ class naming convention, e.g., "UpperCamel". `UPPER_UNDERSCORE` = Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``




## [ExtractNestedField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ExtractNestedField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractNestedField$Value
```


### Configuration

#### General


##### `input.inner.field.name`

The field on the child struct containing the field to be extracted. For example if you wanted the extract `address.state` you would use `state`.

*Importance:* HIGH

*Type:* STRING



##### `input.outer.field.name`

The field on the parent struct containing the child struct. For example if you wanted the extract `address.state` you would use `address`.

*Importance:* HIGH

*Type:* STRING



##### `output.field.name`

The field to place the extracted value into.

*Importance:* HIGH

*Type:* STRING




## [ExtractTimestamp](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ExtractTimestamp.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ExtractTimestamp$Value
```

This transformation is used to use a field from the input data to override the timestamp for the record.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_add_the_timestamp_of_a_field_to_the_topic_name) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=430s)


### Configuration

#### General


##### `field.name`

The field to pull the timestamp from. This must be an int64 or a timestamp.

*Importance:* HIGH

*Type:* STRING




## [HeaderToField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/HeaderToField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.HeaderToField$Value
```


### Configuration

#### General


##### `header.mappings`

The mapping of the header to the field in the message.

*Importance:* HIGH

*Type:* LIST




## [NormalizeSchema](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/NormalizeSchema.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.NormalizeSchema$Value
```

This transformation is used to convert older schema versions to the latest schema version. This works by keying all of the schemas that are coming into the transformation by their schema name and comparing the version() of the schema. The latest version of a schema will be used. Schemas are discovered as the flow through the transformation. The latest version of a schema is what is used.
### Configuration



## [PatternFilter](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/PatternFilter.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternFilter$Value
```


### Configuration

#### General


##### `pattern`

The regex to test the message with. 

*Importance:* HIGH

*Type:* STRING

*Validator:* com.github.jcustenborder.kafka.connect.utils.config.validators.PatternValidator@4170ee0f



##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [PatternRename](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/PatternRename.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value
```


### Configuration

#### General


##### `field.pattern`



*Importance:* HIGH

*Type:* STRING



##### `field.replacement`



*Importance:* HIGH

*Type:* STRING



##### `field.pattern.flags`



*Importance:* LOW

*Type:* LIST

*Default Value:* [CASE_INSENSITIVE]

*Validator:* [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES]




## [SchemaNameToTopic](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SchemaNameToTopic.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SchemaNameToTopic$Value
```

This transformation is used to take the name from the schema for the key or value and replace the topic with this value.
### Configuration



## [SetMaximumPrecision](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SetMaximumPrecision.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SetMaximumPrecision$Value
```

This transformation is used to ensure that all decimal fields in a struct are below the maximum precision specified.
### Note

The Confluent AvroConverter uses a default precision of 64 which can be too large for some database systems.
### Configuration

#### General


##### `precision.max`

The maximum precision allowed.

*Importance:* HIGH

*Type:* INT

*Validator:* [1,...,64]




## [SetNull](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/SetNull.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.SetNull$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.SetNull$Value
```


### Configuration



## [TimestampNow](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TimestampNow.html)

```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNow
```

This transformation is used to override the timestamp of the incoming record to the time the record is being processed.
### Configuration



## [TimestampNowField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TimestampNowField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Value
```

This transformation is used to set a field with the current timestamp of the system running the transformation.

[✍️ Example](https://rmoff.net/2020/12/23/twelve-days-of-smt-day-12-community-transformations/#_add_the_current_timestamp_to_the_message_payload) / [🎥 Video](https://www.youtube.com/watch?v=Z7k_6vGRrkc&t=679s)


### Configuration

#### General


##### `fields`

The field(s) that will be inserted with the timestamp of the system.

*Importance:* HIGH

*Type:* LIST




## [ToJSON](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ToJSON.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ToJSON$Value
```


### Configuration

#### General


##### `output.schema.type`

The connect schema type to output the converted JSON as.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* STRING

*Validator:* [STRING, BYTES]



##### `schemas.enable`

Flag to determine if the JSON data should include the schema.

*Importance:* MEDIUM

*Type:* BOOLEAN




## [ToLong](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/ToLong.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.ToLong$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.ToLong$Value
```


### Configuration

#### General


##### `fields`

The fields to transform.

*Importance:* HIGH

*Type:* LIST




## [TopicNameToField](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-transform-common/transformations/TopicNameToField.html)

*Key*
```
com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Key
```
*Value*
```
com.github.jcustenborder.kafka.connect.transform.common.TopicNameToField$Value
```


### Configuration

#### General


##### `field`

The field to insert the topic name.

*Importance:* HIGH

*Type:* STRING





# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcomed! Before you start any development please create an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.
