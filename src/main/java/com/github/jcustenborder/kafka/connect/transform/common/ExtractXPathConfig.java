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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractXPathConfig extends AbstractConfig {
  private static final Logger log = LoggerFactory.getLogger(ExtractXPathConfig.class);

  public final String inputField;
  public final String outputField;
  public final String xpath;
  public final boolean namespaceAware;
  public final List<String> prefixes;
  public final List<String> namespaces;

  public static final String IN_FIELD_CONFIG = "input.field";
  public static final String IN_FIELD_DOC = "The input field containing the XML Document.";
  public static final String OUT_FIELD_CONFIG = "output.field";
  public static final String OUT_FIELD_DOC = "The output field where the XML element matching the XPath will be placed.";
  public static final String NS_PREFIX_CONFIG = "ns.prefix";
  public static final String NS_PREFIX_DOC = "A comma separated list of Namespace prefixes";
  public static final String NS_LIST_CONFIG = "ns.namespace";
  public static final String NS_LIST_DOC = "A comma separated list of Namespaces corresponding to the prefixes";
  public static final String XPATH_CONFIG = "xpath";
  public static final String XPATH_DOC = "The XPath to apply to extract an element from the Document";



  public ExtractXPathConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.inputField = getString(IN_FIELD_CONFIG);    
    this.outputField = getString(OUT_FIELD_CONFIG);
    this.xpath = getString(XPATH_CONFIG);
    String prefixString = getString(NS_PREFIX_CONFIG);
    String namespaceString = getString(NS_LIST_CONFIG);
    if (prefixString == null || prefixString.trim().length() == 0) {
      this.namespaceAware = false;
      prefixes = new ArrayList<String>();
      namespaces = new ArrayList<String>();
    } else {
      this.namespaceAware = true;
      prefixes = Arrays.asList(prefixString.split(","));
      namespaces = Arrays.asList(namespaceString.split(","));
      if (namespaces.size() != prefixes.size()) {
        log.warn("The list of namespaces and corresponding prefixes are not the same length.");
      }
    }
  }

  public static ConfigDef config() {
    return new ConfigDef()
    .define(IN_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, IN_FIELD_DOC)
    .define(OUT_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, OUT_FIELD_DOC)
    .define(XPATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, XPATH_DOC)
    .define(NS_LIST_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, NS_LIST_DOC)
    .define(NS_PREFIX_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, NS_PREFIX_DOC);
  }

}
