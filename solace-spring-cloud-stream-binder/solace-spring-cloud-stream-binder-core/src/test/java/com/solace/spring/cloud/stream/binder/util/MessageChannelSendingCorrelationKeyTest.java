package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageChannelSendingCorrelationKeyTest {
	private final ErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();

	@Test
	public void testSendResponse() {
		Message<?> message = MessageBuilder.withPayload("test").build();
		MessageChannelSendingCorrelationKey key = new MessageChannelSendingCorrelationKey(message,
				null, null, errorMessageStrategy);
		assertThat(key.sendResponse()).isFalse();
	}

	@Test
	public void testSendResponseChannel() throws Exception {
		Message<?> message = MessageBuilder.withPayload("test").build();
		DirectChannel responseChannel = new DirectChannel();
		MessageChannelSendingCorrelationKey key = new MessageChannelSendingCorrelationKey(message,
				responseChannel, null, errorMessageStrategy);

		SoftAssertions softly = new SoftAssertions();
		CountDownLatch latch = new CountDownLatch(1);
		responseChannel.subscribe(msg -> {
			softly.assertThat(msg).isEqualTo(message);
			latch.countDown();
		});

		assertThat(key.sendResponse()).isTrue();
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		softly.assertAll();
	}

	@Test
	public void testSendError() {
		Message<?> message = MessageBuilder.withPayload("test").build();
		MessageChannelSendingCorrelationKey key = new MessageChannelSendingCorrelationKey(message,
				null, null, errorMessageStrategy);

		String description = "some failure";
		Exception cause = new RuntimeException("test");

		MessagingException exception = key.sendError(description, cause);
		assertThat(exception).hasMessageStartingWith(description);
		assertThat(exception).hasCause(cause);
		assertThat(exception.getFailedMessage()).isEqualTo(message);
	}

	@Test
	public void testSendErrorChannel() throws Exception {
		Message<?> message = MessageBuilder.withPayload("test").build();
		DirectChannel errorChannel = new DirectChannel();
		MessageChannelSendingCorrelationKey key = new MessageChannelSendingCorrelationKey(message,
				null, errorChannel, errorMessageStrategy);

		String description = "some failure";
		Exception cause = new RuntimeException("test");

		SoftAssertions softly = new SoftAssertions();
		CountDownLatch latch = new CountDownLatch(1);
		errorChannel.subscribe(msg -> {
			softly.assertThat(msg).isInstanceOf(ErrorMessage.class);
			ErrorMessage errorMsg = (ErrorMessage) msg;
			softly.assertThat(errorMsg.getOriginalMessage()).isEqualTo(message);
			softly.assertThat(errorMsg.getPayload()).isInstanceOf(MessagingException.class);
			softly.assertThat(errorMsg.getPayload()).hasMessageStartingWith(description);
			softly.assertThat(errorMsg.getPayload()).hasCause(cause);
			softly.assertThat(((MessagingException) errorMsg.getPayload()).getFailedMessage()).isEqualTo(message);
			latch.countDown();
		});

		MessagingException exception = key.sendError(description, cause);
		assertThat(exception).hasMessageStartingWith(description);
		assertThat(exception).hasCause(cause);
		assertThat(exception.getFailedMessage()).isEqualTo(message);
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		softly.assertAll();
	}

	@SuppressWarnings("ThrowableNotThrown")
	@Test
	public void testRawMessageHeader() {
		Message<?> message = MessageBuilder.withPayload("test").build();
		DirectChannel errorChannel = new DirectChannel();
		MessageChannelSendingCorrelationKey key = new MessageChannelSendingCorrelationKey(message, errorChannel, null,
				errorMessageStrategy);
		key.setRawMessage(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));

		SoftAssertions softly = new SoftAssertions();
		errorChannel.subscribe(msg -> {
			softly.assertThat(msg.getHeaders()).containsKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
			softly.assertThat((Object) StaticMessageHeaderAccessor.getSourceData(msg)).isEqualTo(key.getRawMessage());
		});

		key.sendError("some failure", new RuntimeException("test"));
		softly.assertAll();
	}
}
