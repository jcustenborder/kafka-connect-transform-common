package com.github.jcustenborder.kafka.connect.utils.config.validators;

import com.google.common.base.Joiner;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ValidChronoUnit implements ConfigDef.Validator {
    List<String> validOptions = Arrays.stream(ChronoUnit.values()).map(ChronoUnit::name).collect(Collectors.toList());

    @Override
    public void ensureValid(String s, Object o) {
        if (o instanceof String) {
            if (!validOptions.contains(o)) {
                throw new ConfigException(
                        s,
                        String.format(
                                "'%s' is not a valid value for %s. Valid values are %s.",
                                o,
                                ChronoUnit.class.getSimpleName(),
                                Joiner.on(", ").join(validOptions)
                        )
                );
            }
        } else if (o instanceof List) {
            List list = (List) o;
            for (Object i : list) {
                ensureValid(s, i);
            }
        } else {
            throw new ConfigException(
                    s,
                    o,
                    "Must be a String or List"
            );
        }
    }

    @Override
    public String toString() {
        return "Matches: ``" + Joiner.on("``, ``").join(this.validOptions) + "``";
    }
}
