package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;

public class JCSMPAcknowledgementCallbackFactory {
	private final FlowReceiverContainer flowReceiverContainer;
	private final boolean hasTemporaryQueue;
	private final RetryableTaskService taskService;
	private ErrorQueueInfrastructure errorQueueInfrastructure;

	public JCSMPAcknowledgementCallbackFactory(FlowReceiverContainer flowReceiverContainer, boolean hasTemporaryQueue,
											   RetryableTaskService taskService) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.hasTemporaryQueue = hasTemporaryQueue;
		this.taskService = taskService;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public AcknowledgmentCallback createCallback(MessageContainer messageContainer) {
		return new JCSMPAcknowledgementCallback(messageContainer, flowReceiverContainer, hasTemporaryQueue,
				taskService, errorQueueInfrastructure);
	}

	static class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
		private final MessageContainer messageContainer;
		private final FlowReceiverContainer flowReceiverContainer;
		private final boolean hasTemporaryQueue;
		private final ErrorQueueInfrastructure errorQueueInfrastructure;
		private final RetryableTaskService taskService;
		private boolean acknowledged = false;
		private boolean autoAckEnabled = true;

		private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallback.class);

		JCSMPAcknowledgementCallback(MessageContainer messageContainer, FlowReceiverContainer flowReceiverContainer,
									 boolean hasTemporaryQueue,
									 RetryableTaskService taskService,
									 @Nullable ErrorQueueInfrastructure errorQueueInfrastructure) {
			this.messageContainer = messageContainer;
			this.flowReceiverContainer = flowReceiverContainer;
			this.hasTemporaryQueue = hasTemporaryQueue;
			this.taskService = taskService;
			this.errorQueueInfrastructure = errorQueueInfrastructure;
		}

		@Override
		public void acknowledge(Status status) {
			// messageContainer.isAcknowledged() might be async set which is why we also need a local ack variable
			if (acknowledged || messageContainer.isAcknowledged()) {
				logger.info(String.format("%s %s is already acknowledged", XMLMessage.class.getSimpleName(),
						messageContainer.getMessage().getMessageId()));
				return;
			}

			try {
				switch (status) {
					case ACCEPT:
						flowReceiverContainer.acknowledge(messageContainer);
						break;
					case REJECT:
						if (republishToErrorQueue()) {
							break;
						} else if (!hasTemporaryQueue) {
							acknowledge(Status.REQUEUE);
						} else {
							logger.info(String.format(
									"Cannot %s %s %s since this flow is bound to a temporary queue, failed message " +
											"will be discarded",
									Status.REQUEUE, XMLMessage.class.getSimpleName(),
									messageContainer.getMessage().getMessageId()));
							flowReceiverContainer.acknowledge(messageContainer);
						}
						break;
					case REQUEUE:
						if (hasTemporaryQueue) {
							throw new UnsupportedOperationException(String.format(
									"Cannot %s XMLMessage %s, this operation is not supported with temporary queues",
									status, messageContainer.getMessage().getMessageId()));
						} else if (messageContainer.isStale()) {
							throw new SolaceStaleMessageException(String.format(
									"Message container %s (XMLMessage %s) is stale",
									messageContainer.getId(), messageContainer.getMessage().getMessageId()));
						} else {
							logger.info(String.format("%s %s: Will be re-queued onto queue %s",
									XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
									flowReceiverContainer.getQueueName()));
							RetryableAckRebindTask rebindTask = new RetryableAckRebindTask(flowReceiverContainer,
									messageContainer, taskService);
							if (!rebindTask.run(0)) {
								taskService.submit(rebindTask);
							}
						}
				}
			} catch (SolaceAcknowledgmentException e) {
				throw e;
			} catch (Exception e) {
				throw new SolaceAcknowledgmentException(String.format("Failed to acknowledge XMLMessage %s",
						messageContainer.getMessage().getMessageId()), e);
			}

			acknowledged = true;
		}

		/**
		 * Send the message to the error queue and acknowledge the message.
		 * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not defined.
		 */
		private boolean republishToErrorQueue() throws SolaceStaleMessageException {
			if (errorQueueInfrastructure == null) {
				return false;
			}

			logger.info(String.format("%s %s: Will be republished onto error queue %s",
					XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
					errorQueueInfrastructure.getErrorQueueName()));

			if (messageContainer.isStale()) {
				throw new SolaceStaleMessageException(String.format("Cannot republish failed message container %s " +
								"(XMLMessage %s) to error queue %s. Message is stale and will be redelivered.",
						messageContainer.getId(), messageContainer.getMessage().getMessageId(),
						errorQueueInfrastructure.getErrorQueueName()));
			}

			errorQueueInfrastructure.createCorrelationKey(messageContainer, flowReceiverContainer, hasTemporaryQueue)
					.handleError(false);
			return true;
		}

		@Override
		public boolean isAcknowledged() {
			return acknowledged || messageContainer.isAcknowledged();
		}

		@Override
		public void noAutoAck() {
			autoAckEnabled = false;
		}

		@Override
		public boolean isAutoAck() {
			return autoAckEnabled;
		}
	}
}
