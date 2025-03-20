# Changelog

All notable changes to this project will be documented in this file.


## [5.0.7] - 2025-03-21
### Added
- logging of flowId on consumer and producer

### Changed
- One flow per binding, multiple threads if concurrency is set, push from jcsmp library instead of polling
- Updated Libraries

## [5.0.6] - 2025-02-07
### Fixed
- micrometer trace header on specified header field "traceparent"
- support @EnableTestBinder - do not load JCSMPSession if not needed
- support @DirtiesContext - clean JCSMPSession cache on destroy

## [5.0.5] - 2025-01-28
### Updated
- spring boot to 3.4.2
- spring cloud to 2024.0.0

### Fixed
- (solace/merged) DATAGO-69335: Fix for header having value of type byte[] or ByteArray (#338)
- (solace/merged) DATAGO-68275: fix SolaceErrorMessageHandler acknowledgmentCallback detection and error handling (#331)
- (solace/merged) DATAGO-82456: fix queueAdditionalSubscriptions when addDestinationAsSubscription=false (#325)
- (solace/merged) OAuth2 Login

## [5.0.3] - 2024-12-02
### Fixed
- Ensure subscriptions on temporary queues after reconnect
- Fix NPE when tracing is enabled and not tracing header on the message

## [5.0.2] - 2024-09-19
### Added
- Support for Micrometer Tracing

## [5.0.1] - 2024-07-29
### Added
- Support for tests without excluding Autoconfig

## [5.0.0] - 2024-07-28
### Added
- Large message support

### Changed
- Harmonized Logging to SLF4J

### Removed
- Batch processing
- Transactions on batch processing
- Pollable message sources
- TopicEndpoint

## [4.2.4] - 2024-07-22
### Changed
- Change Bean name of context to jcsmpContext to avoid name clashes with jooq

## [4.2.3] - 2024-07-19
### Added
- Cache JCSMPSessions and provide them as Bean to avoid multiple connections to the same broker.
- Add .editorconfig and reformat the whole code.

### Changed
- Bump versions to spring boot 3.3.2
- Bump versions to spring cloud 2023.0.3
- Bump versions of solace jcsmp to 10.24.1
- Use only Slf4j to log.

## [4.2.2] - 2024-07-04
### Changed
- Bump versions of solace jcsmp to 10.24.0

## [4.2.1] - 2024-06-24
### Changed
- Bump versions to spring boot 3.3.1
### Fixed
- Fix a bug when sending Direct Messages

## [4.2.0] - 2024-06-12
### Changed
- By Solace #290 migrate to producer bindings to use JCSMP producer flows
- By Solace #269 give consumer binding threads readable names
- By Solace #294 DATAGO-76828: add transacted producer support
- Bump versions to spring boot 3.3.0 and spring cloud 2023.0.2

## [4.0.1] - 2024-05-13
### Fixed

- Initialisation error of health indicator resulting in a NPE when checking /actuator/health too early.
- Exception on shutdown after trying to reconnect to the broker for some minutes.

## [4.0.0] - 2024-05-07

### Added

- Support for non-persistent publish and subscribe.
- Support for groups in direct subscription using #share subscription on topics. (https://docs.solace.com/Messaging/Direct-Msg/Direct-Messages.htm -> Shared Subscriptions )
- NACK Support for Consumer bindings (by solace https://github.com/SolaceProducts/solace-spring-cloud/pull/270).
- Reapply subscriptions on temporary queues after reconnect with more than 60 sec interruption.

### Fixed

- Startup error with anonymous queues when broker is under load  (fix: https://github.com/SolaceProducts/solace-spring-cloud/issues/266).

### Changed

- Flatten maven structure into a single project (no need for dependency management or starter).
- Upgrade dependencies:
  - spring-boot: 3.2.5
  - spring-cloud: 2023.0.1
  - sol-jcsmp: 10.23.0
  - others: to latest version
- Deprecated batch messaging processing.

### Removed

- git submodule: solace-integration-test-support (integrated into the test now).
- multimodule with starter (integrated into then main project).

## before 4.0.0

Check forked repository https://github.com/SolaceProducts/solace-spring-cloud