# Header-to-User-Property Key Mapping

This document describes the configurable header-to-user-property key mapping feature in the Solace Spring Cloud Stream Binder.

## Overview

The Solace Spring Cloud Stream Binder allows you to configure custom mappings between Spring Message header names and JCSMP User Property keys. This feature provides flexibility in how headers are transmitted and received between Spring Cloud Stream applications and Solace message brokers.

## Configuration

### Property Name
`spring.cloud.stream.solace.headerToUserPropertyKeyMapping`

### Property Type
`Map<String, String>` - A key-value mapping where:
- **Key**: Spring Message header name
- **Value**: JCSMP User Property key

### Configuration Example

```yaml
spring:
  cloud:
    stream:
      solace:
        headerToUserPropertyKeyMapping:
          my-timestamp: timestamp
          source-application: app
          request-id: reqId
          correlation-key: correlationId
```

With this configuration:
- The header `my-timestamp` will be mapped to user property `timestamp`
- The header `source-application` will be mapped to user property `app`
- The header `request-id` will be mapped to user property `reqId`
- The header `correlation-key` will be mapped to user property `correlationId`

## Behavior

### Producer Side (Spring to Solace)
When publishing messages from Spring Cloud Stream to Solace:
1. If a header has a configured mapping, the user property will use the mapped key
2. If no mapping is configured for a header, the original header name is used as the user property key
3. Headers with complex/object values are skipped and a warning is logged
4. If multiple headers map to the same user property key, a warning is logged and the last mapping is used

### Consumer Side (Solace to Spring)
When consuming messages from Solace to Spring Cloud Stream:
1. User properties are mapped back to headers using the reverse of the configured mapping
2. If no mapping exists for a user property, the original user property key is used as the header name
3. If multiple user properties would map to the same header, a warning is logged and the last mapping is used

## Edge Cases and Warnings

### Duplicate Mappings
If multiple headers are mapped to the same user property key:

```yaml
spring:
  cloud:
    stream:
      solace:
        headerToUserPropertyKeyMapping:
          header1: same-key
          header2: same-key  # This creates a duplicate mapping
```

**Behavior**: A warning is logged, and the value from the last processed header (`header2`) will be used.

**Warning Log**: `Duplicate mapping: multiple headers map to user property 'same-key'. Using value from header 'header2'.`

### Complex/Object Values
Headers with complex object values (not String, Number, Boolean, or null) cannot be directly mapped:

```java
// This header will be skipped
message.setHeader("complex-object", new MyCustomObject());
```

**Behavior**: The header is skipped, and a warning is logged.

**Warning Log**: `Header 'complex-object' mapped to user property 'complex-prop' has complex/object value of type 'MyCustomObject'. Skipping mapping.`

### Reverse Mapping Conflicts
On the consumer side, if multiple user properties would map to the same header:

**Warning Log**: `Duplicate reverse mapping: user property 'timestamp' maps to header 'my-timestamp' which conflicts with existing header.`

## Backward Compatibility

This feature is fully backward compatible:
- If no `headerToUserPropertyKeyMapping` is configured, the binder behaves exactly as before
- Setting the property to `null` or an empty map has the same effect as not configuring it
- Existing applications will continue to work without any changes

## Best Practices

### 1. Use Descriptive Mappings
Map verbose header names to shorter, more efficient user property keys:

```yaml
headerToUserPropertyKeyMapping:
  application-correlation-identifier: correlationId
  message-processing-timestamp: timestamp
  source-service-identifier: sourceId
```

### 2. Avoid Reserved Names
While not enforced, avoid mapping to user property keys that might conflict with Solace system properties.

### 3. Consider Message Size
Shorter user property keys can help reduce message overhead, especially for high-throughput applications.

### 4. Document Your Mappings
Maintain documentation of your header mappings to ensure consistency across applications and teams.

## Example Scenarios

### Scenario 1: Cross-System Integration
When integrating with legacy systems that expect specific property names:

```yaml
spring:
  cloud:
    stream:
      solace:
        headerToUserPropertyKeyMapping:
          spring-message-id: MSG_ID
          spring-timestamp: TS
          spring-source: SRC_SYS
```

### Scenario 2: Protocol Translation
When acting as a bridge between different messaging protocols:

```yaml
spring:
  cloud:
    stream:
      solace:
        headerToUserPropertyKeyMapping:
          http-request-id: requestId
          http-session-id: sessionId
          http-user-agent: userAgent
```

### Scenario 3: Message Size Optimization
Reducing header overhead for high-volume messaging:

```yaml
spring:
  cloud:
    stream:
      solace:
        headerToUserPropertyKeyMapping:
          very-long-descriptive-header-name: vh
          another-verbose-header-name: av
          processing-correlation-identifier: pci
```

## Troubleshooting

### Issue: Headers Not Appearing as Expected
**Check**: Verify the mapping configuration syntax and ensure header names match exactly.

### Issue: Warnings About Complex Objects
**Solution**: Ensure header values are simple types (String, Number, Boolean) or consider using JSON serialization for complex objects before setting them as headers.

### Issue: Missing Headers on Consumer Side
**Check**: Verify that the same mapping configuration is applied on both producer and consumer applications, or that the reverse mapping is correctly configured.

### Issue: Duplicate Mapping Warnings
**Solution**: Review your configuration to ensure each header maps to a unique user property key.

## Migration Guide

To adopt this feature in existing applications:

1. **Identify Current Header Usage**: Review your application to understand which headers are currently being used.

2. **Plan Your Mappings**: Decide which headers would benefit from custom mapping (e.g., for size optimization or legacy system compatibility).

3. **Update Configuration**: Add the `headerToUserPropertyKeyMapping` configuration to your application properties.

4. **Test Thoroughly**: Ensure both producer and consumer applications work correctly with the new mappings.

5. **Monitor Logs**: Watch for any warning messages about duplicate mappings or complex object values during initial deployment.

## Limitations

1. **Simple Values Only**: Only headers with simple values (String, Number, Boolean, null) can be mapped. Complex objects are skipped.

2. **Case Sensitivity**: Header names and user property keys are case-sensitive.

3. **Global Configuration**: The mapping applies to all bindings within an application. Per-binding configuration is not supported.

4. **Reserved Headers**: Some Solace-specific headers cannot be remapped as they have special handling within the binder.
