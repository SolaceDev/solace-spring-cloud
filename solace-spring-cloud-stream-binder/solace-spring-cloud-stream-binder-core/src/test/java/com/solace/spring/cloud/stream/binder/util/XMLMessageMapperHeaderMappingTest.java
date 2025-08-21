package com.solace.spring.cloud.stream.binder.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.solace.spring.cloud.stream.binder.properties.SmfMessageWriterProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

@ExtendWith(MockitoExtension.class)
class XMLMessageMapperHeaderMappingTest {

  @Spy
  private XMLMessageMapper xmlMessageMapper;

  @Test
  void testApplyHeaderKeyMapping_SpringHeadersToSolaceUserPropertyKeyMapping() throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
    headerToUserPropertyKeyMapping.put("original-app-header", "app");

    Message<?> springMessage = MessageBuilder.withPayload("test")
        .setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
        .setHeader("original-app-header", "test-app")
        .setHeader("original-unmapped-header", "some-value").build();

    SolaceProducerProperties producerProperties = new SolaceProducerProperties();
    producerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);
    SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
        producerProperties);

    XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

    SDTMap properties = xmlMessage.getProperties();
    assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));
    assertEquals("test-app", properties.getString("app"));

    assertEquals("2025-01-01T00:00:00Z", properties.getString("original-timestamp-header"));
    assertEquals("test-app", properties.getString("original-app-header"));
    assertEquals("some-value", properties.getString("original-unmapped-header"));
  }

  @Test
  void testApplyHeaderKeyMapping_DuplicateSpringHeadersToSolaceUserPropertyKeyMapping()
      throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
    headerToUserPropertyKeyMapping.put("original-app-header", "timestamp");

    Message<?> springMessage = MessageBuilder.withPayload("test")
        .setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
        .setHeader("original-app-header", "test-app")
        .setHeader("original-unmapped-header", "some-value").build();

    SolaceProducerProperties producerProperties = new SolaceProducerProperties();
    producerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);
    SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
        producerProperties);

    XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

    //Log contains warning about duplicate mapping
    SDTMap properties = xmlMessage.getProperties();
    assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));

    assertEquals("2025-01-01T00:00:00Z", properties.getString("original-timestamp-header"));
    assertEquals("test-app", properties.getString("original-app-header"));
    assertEquals("some-value", properties.getString("original-unmapped-header"));
  }

  @Test
  void testApplyHeaderKeyMapping_SolaceUserPropertiesToSpringHeadersMapping() throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("id", "mapped-id-header");
    headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
    headerToUserPropertyKeyMapping.put("app", "mapped-app-header");

    TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    xmlMessage.setText("test");
    SDTMap properties = JCSMPFactory.onlyInstance().createMap();
    properties.putString("id", "12345");
    properties.putString("timestamp", "2025-01-01T00:00:00Z");
    properties.putString("app", "test-app");
    properties.putString("unmapped-user-prop", "some-value");
    xmlMessage.setProperties(properties);

    SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
    consumerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);

    Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, consumerProperties);

    assertEquals("12345", springMessage.getHeaders().get("mapped-id-header"));
    assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));
    assertEquals("test-app", springMessage.getHeaders().get("mapped-app-header"));

    //id and timestamp is Spring Integration reserved headers, original values will not be preserved
    assertNotEquals("12345", springMessage.getHeaders().get("id"));
    assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
    assertNotNull(springMessage.getHeaders().get("id"));
    assertNotNull(springMessage.getHeaders().get("timestamp"));
    assertEquals("test-app", springMessage.getHeaders().get("app"));
    assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));
  }

  @Test
  void testApplyHeaderKeyMapping_DuplicateSolaceUserPropertiesToSpringHeadersMapping()
      throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
    headerToUserPropertyKeyMapping.put("app", "mapped-timestamp-header");

    TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    xmlMessage.setText("test");
    SDTMap properties = JCSMPFactory.onlyInstance().createMap();
    properties.putString("timestamp", "2025-01-01T00:00:00Z");
    properties.putString("app", "test-app");
    properties.putString("unmapped-user-prop", "some-value");
    xmlMessage.setProperties(properties);

    SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
    consumerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);

    Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, consumerProperties);

    assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));

    //id and timestamp is Spring Integration reserved headers, original values will not be preserved
    assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
    assertNotNull(springMessage.getHeaders().get("timestamp"));
    assertEquals("test-app", springMessage.getHeaders().get("app"));
    assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));
  }

  @ParameterizedTest(name = "{index} testApplyHeaderKeyMapping_ProducerBackwardCompatibility - {1}")
  @MethodSource("publisherBackwardCompatibilityArguments")
  void testApplyHeaderKeyMapping_ProducerBackwardCompatibility(
      SolaceProducerProperties producerProperties, String scenario) throws SDTException {
    Message<?> springMessage = MessageBuilder.withPayload("test").setHeader("my-header", "my-value")
        .build();

    SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
        producerProperties);
    XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

    verify(xmlMessageMapper).applyHeaderKeyMapping(anyMap(),
        eq(writerProperties.getHeaderToUserPropertyKeyMapping()));

    SDTMap properties = xmlMessage.getProperties();
    assertEquals("my-value", properties.getString("my-header"));
  }

  private static Stream<Arguments> publisherBackwardCompatibilityArguments() {
    SolaceProducerProperties producerPropertiesWithEmptyHeaderMapping = new SolaceProducerProperties();
    producerPropertiesWithEmptyHeaderMapping.setHeaderToUserPropertyKeyMapping(
        new LinkedHashMap<>());

    return Stream.of(arguments(new SolaceProducerProperties(), "Default Producer Properties"),
        arguments(producerPropertiesWithEmptyHeaderMapping,
            "Producer Properties with Empty Mapping"));
  }

  @ParameterizedTest(name = "{index} testApplyHeaderKeyMapping_ConsumerBackwardCompatibility - {1}")
  @MethodSource("consumerBackwardCompatibilityArguments")
  void testApplyHeaderKeyMapping_ConsumerBackwardCompatibility(
      SolaceConsumerProperties solaceConsumerProperties, String scenario) throws SDTException {
    TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    xmlMessage.setText("test");
    SDTMap properties = JCSMPFactory.onlyInstance().createMap();
    properties.putString("my-header", "my-value");
    xmlMessage.setProperties(properties);

    Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null,
        solaceConsumerProperties);

    verify(xmlMessageMapper).applyHeaderKeyMapping(any(SDTMap.class), anyMap());

    assertEquals("my-value", springMessage.getHeaders().get("my-header"));
  }

  private static Stream<Arguments> consumerBackwardCompatibilityArguments() {
    SolaceConsumerProperties consumerPropertiesWithEmptyHeaderMapping = new SolaceConsumerProperties();
    consumerPropertiesWithEmptyHeaderMapping.setHeaderToUserPropertyKeyMapping(
        new LinkedHashMap<>());

    return Stream.of(arguments(new SolaceConsumerProperties(), "Default Consumer Properties"),
        arguments(consumerPropertiesWithEmptyHeaderMapping,
            "Consumer Properties with Empty Mapping"));
  }

  @Test
  void testApplyHeaderKeyMapping_MultipleMappingsAndTypes() throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("string-header", "str");
    headerToUserPropertyKeyMapping.put("number-header", "num");
    headerToUserPropertyKeyMapping.put("boolean-header", "bool");

    // Create test message with different types
    Message<?> springMessage = MessageBuilder.withPayload("test")
        .setHeader("string-header", "string-value").setHeader("number-header", 42)
        .setHeader("boolean-header", true).setHeader("unmapped-header", "unmapped-value").build();

    SolaceProducerProperties producerProperties = new SolaceProducerProperties();
    producerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);
    SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
        producerProperties);
    XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

    // Verify all mappings work correctly
    SDTMap properties = xmlMessage.getProperties();
    assertEquals("string-value", properties.getString("str"));
    assertEquals(42, properties.getInteger("num").intValue());
    assertEquals(true, properties.getBoolean("bool"));
    assertEquals("unmapped-value", properties.getString("unmapped-header"));
  }

  @Test
  void testApplyHeaderKeyMapping_producerHeaderExclusionWithMapping() throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
    headerToUserPropertyKeyMapping.put("original-app-header", "app");

    List<String> excludedHeaders = List.of("original-timestamp-header", "original-app-header");

    Message<?> springMessage = MessageBuilder.withPayload("test")
        .setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
        .setHeader("original-app-header", "test-app")
        .setHeader("original-unmapped-header", "some-value").build();

    SolaceProducerProperties producerProperties = new SolaceProducerProperties();
    producerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);
    producerProperties.setHeaderExclusions(excludedHeaders);
    SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
        producerProperties);

    XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

    SDTMap properties = xmlMessage.getProperties();
    assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));
    assertEquals("test-app", properties.getString("app"));

    //Excluded headers should not be present in the properties
    assertNull(properties.getString("original-timestamp-header"));
    assertNull(properties.getString("original-app-header"));
    assertEquals("some-value", properties.getString("original-unmapped-header"));
  }

  @Test
  void testApplyHeaderKeyMapping_consumerHeaderExclusionWithMapping() throws SDTException {
    Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
    headerToUserPropertyKeyMapping.put("id", "mapped-id-header");
    headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
    headerToUserPropertyKeyMapping.put("app", "mapped-app-header");

    List<String> excludedHeaders = List.of("id", "timestamp");

    TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    xmlMessage.setText("test");
    SDTMap properties = JCSMPFactory.onlyInstance().createMap();
    properties.putString("id", "12345");
    properties.putString("timestamp", "2025-01-01T00:00:00Z");
    properties.putString("app", "test-app");
    properties.putString("unmapped-user-prop", "some-value");
    xmlMessage.setProperties(properties);

    SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
    consumerProperties.setHeaderToUserPropertyKeyMapping(headerToUserPropertyKeyMapping);
    consumerProperties.setHeaderExclusions(excludedHeaders);

    Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, consumerProperties);

    assertEquals("12345", springMessage.getHeaders().get("mapped-id-header"));
    assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));
    assertEquals("test-app", springMessage.getHeaders().get("mapped-app-header"));

    //id and timestamp is Spring Integration reserved headers, original values will not be preserved
    assertNotEquals("12345", springMessage.getHeaders().get("id"));
    assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
    assertNotNull(springMessage.getHeaders().get("id"));
    assertNotNull(springMessage.getHeaders().get("timestamp"));
    assertEquals("test-app", springMessage.getHeaders().get("app"));
    assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));
  }

}