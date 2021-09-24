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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.util.Map;
import java.util.LinkedHashMap;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;

import org.apache.ws.commons.util.NamespaceContextImpl;

public abstract class ExtractXPath<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ExtractXPath.class);
  
  
  ExtractXPathConfig config;
  public ExtractXPathConfig theConfig() {
    return this.config;
  }
  
  DocumentBuilder builder;
  XPath xpath;
  XPathExpression xpathE;
  LSSerializer writer;
  
  @Override
  public ConfigDef config() {
    return ExtractXPathConfig.config();
  }
  
  @Override
  public void close() {
    
  }
  
  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new ExtractXPathConfig(settings);
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(config.namespaceAware);
      builder = factory.newDocumentBuilder();
      xpath = XPathFactory.newInstance().newXPath();
      if (config.namespaceAware) {
        NamespaceContextImpl nsContext = new NamespaceContextImpl();
        for (int i = 0; i < config.prefixes.size(); i++) {
          String prefix = config.prefixes.get(i);
          String ns = config.namespaces.get(i);
          log.debug("Adding prefix {} for namespace {}", prefix, ns);
          nsContext.startPrefixMapping(prefix, ns);
        }
        xpath.setNamespaceContext(nsContext);
      }
      xpathE = xpath.compile(config.xpath);
      
      DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
      DOMImplementationLS impl = (DOMImplementationLS) registry.getDOMImplementation("LS");
      writer = impl.createLSSerializer();
    } catch (Exception e) {
      log.error("Unable to create transformer {} {}", e.getMessage(), e.toString());
    }
  }
  
  /**
  * 
  */
  private Object extractXPathString(Object inData) {
    log.trace("extractXPathString Data type {}", inData.getClass());
    InputStream in = null;
    try {
      
      if (inData instanceof String) {
        log.trace("Handling XML String");
        String inFieldData = (String) inData;
        in = new ByteArrayInputStream(inFieldData.getBytes());
        Document doc = builder.parse(in);
        Node node = (Node) xpathE.evaluate(doc, XPathConstants.NODE);
        String output = writer.writeToString(node);
        return output;  
      } else if (inData instanceof byte[]) {
        byte[] inFieldData = (byte[]) inData;
        log.trace("Handling byte array, length {}", inFieldData.length);
        in = new ByteArrayInputStream(inFieldData);
        Document doc = builder.parse(in);
        Node node = (Node) xpathE.evaluate(doc, XPathConstants.NODE);
        String output = writer.writeToString(node);
        return output.getBytes();  
      } else {
        log.error("Expected a String or byte[], got a {}", inData.getClass().getName());
      }
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      log.error("Unable to evaluate XPath {} {}", e.getMessage(), sw.toString());
    }
    return null;
  }

  @Override
  protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
    Schema outPutSchema = inputSchema;
    SchemaAndValue retVal = null;
    log.trace("process() - Processing bytes");
    Object extractedValue = extractXPathString(input);
    retVal = new SchemaAndValue(outPutSchema, extractedValue);
    return retVal;
  }
  
  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
    Schema outPutSchema = inputSchema;
    SchemaAndValue retVal = null;
    if (!config.outputField.equals(config.inputField)) {
      // Adding a new field, need to build a new schema
      final SchemaBuilder outputSchemaBuilder = SchemaBuilder.struct();
      outputSchemaBuilder.name(inputSchema.name());
      outputSchemaBuilder.doc(inputSchema.doc());
      if (null != inputSchema.defaultValue()) {
        outputSchemaBuilder.defaultValue(inputSchema.defaultValue());
      }
      if (null != inputSchema.parameters() && !inputSchema.parameters().isEmpty()) {
        outputSchemaBuilder.parameters(inputSchema.parameters());
      }
      if (inputSchema.isOptional()) {
        outputSchemaBuilder.optional();
      }
      for (final Field inputField : inputSchema.fields()) {
        final String inputFieldName = inputField.name();
        log.trace("process() - Schema has field '{}'", inputFieldName);
        outputSchemaBuilder.field(inputFieldName, inputField.schema());
        if (inputFieldName.equals(config.inputField)) {
          log.trace("process() - Adding field '{}'", config.outputField);
          outputSchemaBuilder.field(config.outputField, inputField.schema());
        }
      }
      final Schema newSchema = outputSchemaBuilder.build();
      final Struct outputStruct = new Struct(newSchema);
      for (final Field inputField : inputSchema.fields()) {
        final String inputFieldName = inputField.name();
        final Object value = inputStruct.get(inputFieldName);
        outputStruct.put(inputFieldName, value);
        if (inputFieldName.equals(config.inputField)) {
          Object extractedValue = extractXPathString(value);
          outputStruct.put(config.outputField, extractedValue);
        }
      }
      retVal = new SchemaAndValue(newSchema, outputStruct);
    } else {
      Struct outputStruct = inputStruct;
      Object toReplace = inputStruct.get(config.inputField);
      if (toReplace != null && toReplace instanceof String) {
        String inputFieldName = config.inputField;
        String replacedField = (String) toReplace;
        log.trace("process() - Processing struct field '{}' value '{}'", inputFieldName, toReplace);
        Object extractedValue = extractXPathString(replacedField);
        if (config.outputField.equals(config.inputField)) {
          log.debug("process() - Replaced struct field '{}' with '{}'", inputFieldName, extractedValue);
        } else {
          log.debug("process() - Added struct field '{}' with '{}'", config.outputField, extractedValue);
        }
        outputStruct.put(config.outputField, extractedValue);
        retVal = new SchemaAndValue(outPutSchema, outputStruct);
      }
    }
    return retVal;
  }
  
  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    Map<String, Object> outputMap = new LinkedHashMap<>(input.size());
    for (final String inputFieldName : input.keySet()) {
      outputMap.put(inputFieldName, input.get(inputFieldName));
      log.trace("process() - Processing map field '{}' value '{}'", inputFieldName, input.get(inputFieldName));
      if (inputFieldName.equals(config.inputField)) {
        String fieldToMatch = (String) input.get(inputFieldName);
        Object replacedValue = extractXPathString(fieldToMatch);
        outputMap.put(config.outputField, replacedValue);
        if (config.outputField.equals(config.inputField)) {
          log.debug("process() - Replaced map field '{}' with '{}'", inputFieldName, replacedValue);
        } else {
          log.debug("process() - Added map field '{}' with '{}'", config.outputField, replacedValue);
        }
      }
    }
    return new SchemaAndValue(null, outputMap);
  }
  
  @Title("ExtractXPathConfig(Key)")
  @Description("This transformation is used to take structured data such as AVRO and output it as " +
      "JSON by way of the JsonConverter built into Kafka Connect.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends ExtractXPath<R> {
    
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());
      
      return r.newRecord(
      r.topic(),
      r.kafkaPartition(),
      transformed.schema(),
      transformed.value(),
      r.valueSchema(),
      r.value(),
      r.timestamp()
      );
    }
  }
  
  @Title("ExtractXPathConfig(Value)")
  @Description("This transformation is used to take structured data such as AVRO and output it as " +
      "JSON by way of the JsonConverter built into Kafka Connect.")
  public static class Value<R extends ConnectRecord<R>> extends ExtractXPath<R> {
    
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());
      
      return r.newRecord(
      r.topic(),
      r.kafkaPartition(),
      r.keySchema(),
      r.key(),
      transformed.schema(),
      transformed.value(),
      r.timestamp()
      );
    }
  }
  
}
