# Introduction

This project provides some common transformation functionality for Kafka Connect.

# Configuration

## PatternRename$Key

This transformation is used to rename fields in the key of an input struct based on a regular expression and a replacement string.

```properties
transforms=key
transforms.key.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Key

# Set these required values
transforms.key.field.pattern=
transforms.key.field.replacement=
```

| Name                | Description | Type   | Default            | Valid Values                                                                                                          | Importance |
|---------------------|-------------|--------|--------------------|-----------------------------------------------------------------------------------------------------------------------|------------|
| field.pattern       |             | string |                    |                                                                                                                       | high       |
| field.replacement   |             | string |                    |                                                                                                                       | high       |
| field.pattern.flags |             | list   | [CASE_INSENSITIVE] | [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES] | low        |

## PatternRename$Value

This transformation is used to rename fields in the value of an input struct based on a regular expression and a replacement string.

```properties
transforms=value
transforms.value.type=com.github.jcustenborder.kafka.connect.transform.common.PatternRename$Value

# Set these required values
transforms.value.field.pattern=
transforms.value.field.replacement=
```

| Name                | Description | Type   | Default            | Valid Values                                                                                                          | Importance |
|---------------------|-------------|--------|--------------------|-----------------------------------------------------------------------------------------------------------------------|------------|
| field.pattern       |             | string |                    |                                                                                                                       | high       |
| field.replacement   |             | string |                    |                                                                                                                       | high       |
| field.pattern.flags |             | list   | [CASE_INSENSITIVE] | [UNICODE_CHARACTER_CLASS, CANON_EQ, UNICODE_CASE, DOTALL, LITERAL, MULTILINE, COMMENTS, CASE_INSENSITIVE, UNIX_LINES] | low        |
 