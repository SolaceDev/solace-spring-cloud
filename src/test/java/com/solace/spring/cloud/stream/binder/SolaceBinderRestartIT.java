package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPSession;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for testing Solace binder restart functionality.
 * Tests starting bindings without autostart, programmatic start/stop/restart,
 * and message sending/receiving before and after restart.
 */
@Slf4j
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
@DirtiesContext
public class SolaceBinderRestartIT extends SpringCloudStreamContext {

    @BeforeEach
    void setUp(JCSMPSession jcsmpSession, SempV2Api sempV2Api) {
        setJcsmpSession(jcsmpSession);
        setSempV2Api(sempV2Api);
    }

    @AfterEach
    void tearDown() {
        super.cleanup();
        close();
    }

    // NOT YET SUPPORTED ---------------------------------
    @Override
    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    @Disabled("Partitioning is not supported")
    public void testPartitionedModuleSpEL(TestInfo testInfo) throws Exception {
        super.testPartitionedModuleSpEL(testInfo);
    }
    // ---------------------------------------------------

    @Test
    public void testBindingRestartWithMessaging(SoftAssertions softly, TestInfo testInfo) throws Exception {

        log.info("[DEBUG_LOG] Starting testBindingRestartWithMessaging");

        SolaceTestBinder binder = getBinder();
        var consumerInfrastructureUtil = createConsumerInfrastructureUtil(DirectChannel.class);

        // Create channels
        DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
        var moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination = getRandomAlphanumeric();
        String consumerGroup = getRandomAlphanumeric();

        // Create producer binding (this will auto-start)
        Binding<MessageChannel> producerBinding = binder.bindProducer(destination, moduleOutputChannel, createProducerProperties(testInfo));

        // Create consumer binding WITHOUT autostart
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        consumerProperties.setAutoStartup(false);
        var consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination, consumerGroup, moduleInputChannel, consumerProperties);

        log.info("[DEBUG_LOG] Created bindings - producer auto-started, consumer not auto-started");

        // Verify the consumer is not running initially
        assertThat(consumerBinding.isRunning()).isFalse();
        assertThat(consumerBinding.isPaused()).isFalse();

        binderBindUnbindLatency();

        // Step 1: Start the consumer binding programmatically
        log.info("[DEBUG_LOG] Starting consumer binding programmatically");
        consumerBinding.start();
        assertThat(consumerBinding.isRunning()).isTrue();

        // Wait for binding to be fully started
        Thread.sleep(2000);
        log.info("[DEBUG_LOG] Consumer binding started, isRunning: {}, isPaused: {}", consumerBinding.isRunning(), consumerBinding.isPaused());

        // Step 2: Test message flow BEFORE stopping the binding
        log.info("[DEBUG_LOG] Testing message flow before stopping binding");
        testMessageFlow(moduleOutputChannel, moduleInputChannel, consumerInfrastructureUtil, softly, "before-stop");

        // Step 3: Stop the consumer binding programmatically AFTER asserting message flow works
        log.info("[DEBUG_LOG] Stopping consumer binding programmatically after message flow test");
        consumerBinding.stop();
        assertThat(consumerBinding.isRunning()).isFalse();
        log.info("[DEBUG_LOG] Consumer binding stopped, isRunning: {}", consumerBinding.isRunning());

        // Step 4: Start the SAME consumer binding again (not creating a new one)
        log.info("[DEBUG_LOG] Starting the same consumer binding again");
        consumerBinding.start();
        assertThat(consumerBinding.isRunning()).isTrue();

        // Wait for binding to be fully started
        Thread.sleep(3000);
        log.info("[DEBUG_LOG] Same consumer binding restarted, isRunning: {}, isPaused: {}", consumerBinding.isRunning(), consumerBinding.isPaused());

        // Step 5: Test message flow AFTER restarting the same binding
        log.info("[DEBUG_LOG] Testing message flow after restarting the same binding");
        testMessageFlow(moduleOutputChannel, moduleInputChannel, consumerInfrastructureUtil, softly, "after-restart");

        // Cleanup
        log.info("[DEBUG_LOG] Cleaning up bindings");
        consumerBinding.unbind();
        producerBinding.unbind();

        log.info("[DEBUG_LOG] Test completed successfully");
    }

    private static String getRandomAlphanumeric() {
        return RandomGenerator.getDefault().ints(10, 0, 36).mapToObj(i -> Character.toString("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".charAt(i))).collect(Collectors.joining());
    }

    /**
     * Helper method to test message flow
     */
    private void testMessageFlow(DirectChannel moduleOutputChannel, DirectChannel moduleInputChannel, ConsumerInfrastructureUtil<DirectChannel> consumerInfrastructureUtil, SoftAssertions softly, String phase) throws InterruptedException {

        int numMessages = 3;
        List<Message<?>> messages = IntStream.range(0, numMessages).mapToObj(i -> MessageBuilder.withPayload(("test-message-" + phase + "-" + i).getBytes()).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE).build()).collect(Collectors.toList());

        log.info("[DEBUG_LOG] Sending {} messages during phase: {}", numMessages, phase);

        AtomicInteger receivedCount = new AtomicInteger(0);

        // Use the proper sendAndSubscribe method like in the original tests
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, numMessages, () -> messages.forEach(moduleOutputChannel::send), msg -> {
            log.info("[DEBUG_LOG] Received message during {}: {}", phase, new String((byte[]) msg.getPayload()));
            receivedCount.incrementAndGet();
            // Verify message content matches what we sent
            String payload = new String((byte[]) msg.getPayload());
            softly.assertThat(payload).startsWith("test-message-" + phase + "-");
        });

        softly.assertThat(receivedCount.get()).as("Received count should match sent count during phase: " + phase).isEqualTo(numMessages);

        log.info("[DEBUG_LOG] Message flow test completed for phase: {}", phase);
    }
}
