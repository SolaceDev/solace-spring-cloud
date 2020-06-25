package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.util.JCSMPKeepAlive;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

public class JCSMPInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {
	private final String id = UUID.randomUUID().toString();
	private final ConsumerDestination consumerDestination;
	private final JCSMPSession jcsmpSession;
	private final EndpointProperties endpointProperties;
	private final Consumer<Queue> postStart;
	private final JCSMPKeepAlive keepAlive;
	private final int concurrency;
	private final Set<FlowReceiver> consumerFlowReceivers;
	private RetryTemplate retryTemplate;
	private RecoveryCallback<?> recoveryCallback;

	private static final Log logger = LogFactory.getLog(JCSMPInboundChannelAdapter.class);
	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	public JCSMPInboundChannelAdapter(ConsumerDestination consumerDestination,
									  JCSMPSession jcsmpSession,
									  JCSMPKeepAlive keepAlive,
									  int concurrency,
									  @Nullable EndpointProperties endpointProperties,
									  @Nullable Consumer<Queue> postStart) {
		this.consumerDestination = consumerDestination;
		this.jcsmpSession = jcsmpSession;
		this.keepAlive = keepAlive;
		this.concurrency = concurrency;
		this.endpointProperties = endpointProperties;
		this.postStart = postStart;
		this.consumerFlowReceivers = new HashSet<>(this.concurrency);
	}

	@Override
	protected void doStart() {
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Creating %s consumer flows for queue %s <inbound adapter %s>",
				concurrency, queueName, id));

		if (isRunning()) {
			logger.warn(String.format("Nothing to do. Inbound message channel adapter %s is already running", id));
			return;
		}

		if (concurrency < 1) {
			String msg = String.format("Concurrency must be greater than 0, was %s <inbound adapter %s>",
					concurrency, id);
			logger.warn(msg);
			throw new MessagingException(msg);
		}

		if (!consumerFlowReceivers.isEmpty()) {
			logger.warn(String.format("Unexpectedly found %s consumer flows while starting inbound adapter %s, " +
					"closing them...", concurrency, id));
			consumerFlowReceivers.forEach(FlowReceiver::close);
			consumerFlowReceivers.clear();
		}

		XMLMessageListener listener = buildListener();
		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		try {
			final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
			flowProperties.setEndpoint(queue);
			flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			for (int i = 0; i < concurrency; i++) {
				FlowReceiver flowReceiver = jcsmpSession.createFlow(listener, flowProperties, endpointProperties);
				flowReceiver.start();
				consumerFlowReceivers.add(flowReceiver);
			}
		} catch (JCSMPException e) {
			String msg = "Failed to get message consumer from session";
			logger.warn(msg, e);
			throw new MessagingException(msg, e);
		}

		if (postStart != null) {
			postStart.accept(queue);
		}

		keepAlive.create(getClass(), id);
	}

	@Override
	protected void doStop() {
		if (!isRunning()) return;
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Stopping consumer flow from queue %s <inbound adapter ID: %s>", queueName, id));
		consumerFlowReceivers.forEach(FlowReceiver::close);
		consumerFlowReceivers.clear();
		keepAlive.stop(getClass(), id);
	}

	@Override
	public int beforeShutdown() {
		this.stop();
		return 0;
	}

	@Override
	public int afterShutdown() {
		return 0;
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	protected AttributeAccessor getErrorMessageAttributes(org.springframework.messaging.Message<?> message) {
		AttributeAccessor attributes = attributesHolder.get();
		return attributes == null ? super.getErrorMessageAttributes(message) : attributes;
	}

	private XMLMessageListener buildListener() {
		XMLMessageListener listener;
		if (retryTemplate != null) {
			Assert.state(getErrorChannel() == null,
					"Cannot have an 'errorChannel' property when a 'RetryTemplate' is provided; " +
							"use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to send " +
							"an error message when retries are exhausted");
			RetryableInboundXMLMessageListener retryableMessageListener = new RetryableInboundXMLMessageListener(
					consumerDestination,
					this::sendMessage,
					(exception) -> sendErrorMessageIfNecessary(null, exception),
					retryTemplate,
					recoveryCallback,
					attributesHolder
			);
			retryTemplate.registerListener(retryableMessageListener);
			listener = retryableMessageListener;
		} else {
			listener = new InboundXMLMessageListener(
					consumerDestination,
					this::sendMessage,
					(exception) -> sendErrorMessageIfNecessary(null, exception),
					attributesHolder,
					this.getErrorChannel() != null
			);
		}
		return listener;
	}
}
