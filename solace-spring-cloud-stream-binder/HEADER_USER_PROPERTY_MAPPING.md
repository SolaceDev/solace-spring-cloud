# Header to User Property Key Mapping

This document describes the configurable `headerNameMapping` feature in the Solace Spring Cloud Stream Binder.

## Overview

The Solace Spring Cloud Stream Binder allows you to configure custom mappings between Spring Message header names and JCSMP User Property keys and vice versa. This feature provides flexibility in how headers are transmitted and received between Spring Cloud Stream applications and Solace message brokers.

### Configuration Example

Default configuration for all producers and consumers: under `spring.cloud.stream.solace.default`.

```yaml
spring:
  cloud:
    stream:
      solace:
        default:
          consumer:
            headerNameMapping:
              timestamp: my-timestamp
              app: source-application
              reqId: request-id
              correlationId: correlation-key
          producer:
            headerNameMapping:
              my-timestamp: timestamp
              source-application: app
              request-id: reqId
              correlation-key: correlationId
          
```

Per binding specific configuration: under `spring.cloud.stream.solace.<binding-name>`.

```yaml
spring:
  cloud:
    stream:
      solace:
        consumerBindingName-in-0:
          consumer:
            headerNameMapping:
              timestamp: my-timestamp
              app: source-application
              reqId: request-id
              correlationId: correlation-key
        producerBindingName-out-0:
          producer:
            headerNameMapping:
              my-timestamp: timestamp
              source-application: app
              request-id: reqId
              correlation-key: correlationId
```

With this configuration:

Following mapping will be applied when consuming messages from Solace to Spring Cloud Stream:
- The header `timestamp`  will be mapped to user property `my-timestamp`
- The header `app` will be mapped to user property `source-application`
- The header `reqId` will be mapped to user property `request-id`
- The header `correlationId` will be mapped to user property `correlation-key`

and vise versa when producing messages from Spring Cloud Stream to Solace.

## Behavior

### Producer Side (Spring to Solace)
When publishing messages from Spring Cloud Stream to Solace:
1. If a header has a configured mapping, the user property will use the mapped key
2. If no mapping is configured for a header, the original header name is used as the user property key
3. If multiple headers map to the same user property key, a warning is logged and the first mapping is used

### Consumer Side (Solace to Spring)
When consuming messages from Solace to Spring Cloud Stream:
1. User properties are mapped back to headers using the configured mapping
2. If no mapping exists for a user property, the original user property key is used as the header name
3. If multiple user properties would map to the same header, a warning is logged and the first mapping is used


## Backward Compatibility

This feature is fully backward compatible:
- If no `headerNameMapping` is configured, the binder behaves exactly as before
- Existing applications will continue to work without any changes

## Best Practices

### 1. Avoid Reserved Names
While not enforced, avoid mapping to user property keys that might conflict with Solace system properties.

### 2. Consider Message Size
Shorter user property keys can help reduce message overhead, especially for high-throughput applications.

### 3. Document Your Mappings
Maintain documentation of your header mappings to ensure consistency across applications and teams.

## Example Scenarios

### Scenario 1: Cross-System Integration
When integrating with legacy systems that expect specific property names:

```yaml
spring:
  cloud:
    stream:
      solace:
        headerNameMapping:
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
        headerNameMapping:
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
        headerNameMapping:
          very-long-descriptive-header-name: vh
          another-verbose-header-name: av
          processing-correlation-identifier: pci
```