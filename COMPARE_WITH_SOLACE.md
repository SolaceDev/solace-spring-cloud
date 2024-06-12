# Difference between the splace binder and this fork

Additional:
- Support for non-persistent publish and subscribe
- Support for groups in direct subscription using #share subscription on topics.
- Reapply subscriptions on temporary queues after reconnect with more than 60 sec interruption
- Fixed startup error with anonymous queues when broker is under load

Not supported by fork on purpose:
- Batch processing - We judge this feature as unnecessary complex and an anti-pattern
- Selector - This is a JMS feature generating a lot of traffic and the same effect can be done easy by a simple filter
- TopicEndPoint - This is a JMS feature and an anti pattern
- Multi maven project - a simple maven project does the job as well and is less expensive to maintain
