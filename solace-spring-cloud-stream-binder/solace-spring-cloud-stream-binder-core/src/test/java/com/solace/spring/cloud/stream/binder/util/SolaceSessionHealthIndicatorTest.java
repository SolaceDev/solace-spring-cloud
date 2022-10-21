package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthSessionProperties;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.junit.jupiter.api.Assertions.assertNull;

public class SolaceSessionHealthIndicatorTest {
	@Test
	public void testInitialHealth() {
		assertNull(new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties()).health());
	}

	@Test
	public void testUp(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.up();
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.down(null, 0, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@CartesianTest(name = "[{index}] status={0} responseCode={1} info={2}")
	public void testDetails(@Values(strings = {"DOWN", "RECONNECTING"}) String status,
							@Values(ints = {-1, 0, 1}) int responseCode,
							@Values(strings = {"", "some-info"}) String info,
							SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		Exception healthException = new Exception("test");
		switch (status) {
			case "DOWN":
				healthIndicator.down(healthException, responseCode, info);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(healthException, responseCode, info);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.health();

		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));
		softly.assertThat(health.getDetails())
				.isNotEmpty()
				.hasEntrySatisfying("error", error -> Assertions.assertThat(error)
						.isEqualTo(healthException.getClass().getName() + ": " + healthException.getMessage()));

		if (responseCode != 0) {
			softly.assertThat(health.getDetails())
					.hasEntrySatisfying("responseCode", r -> Assertions.assertThat(r).isEqualTo(responseCode));
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("responseCode");
		}

		if (!info.isEmpty()) {
			softly.assertThat(health.getDetails())
					.hasEntrySatisfying("info", i -> Assertions.assertThat(i).isEqualTo(info));
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("info");
		}
	}

	@ParameterizedTest(name = "[{index}] status={0}")
	@ValueSource(strings = {"DOWN", "RECONNECTING"})
	public void testWithoutDetails(String status, SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		switch (status) {
			case "DOWN":
				healthIndicator.down(new RuntimeException("test"), 1, "some-info");
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(new RuntimeException("test"), 1, "some-info");
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.getHealth(false);
		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.reconnecting(null, 0, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(new Status("RECONNECTING"));
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@ParameterizedTest(name = "[{index}] reconnectAttemptsUntilDown={0}")
	@ValueSource(longs = {1, 10})
	public void testReconnectingDownThresholdReached(long reconnectAttemptsUntilDown, SoftAssertions softly) {
		SolaceHealthSessionProperties properties = new SolaceHealthSessionProperties();
		properties.setReconnectAttemptsUntilDown(reconnectAttemptsUntilDown);
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(properties);
		for (int i = 0; i < reconnectAttemptsUntilDown; i++) {
			healthIndicator.reconnecting(null, 0, null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		}

		for (int i = 0; i < 3; i++) {
			healthIndicator.reconnecting(null, 0, null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
		}
	}

	@ParameterizedTest(name = "[{index}] resetStatus={0}")
	@ValueSource(strings = {"DOWN", "UP"})
	public void testReconnectingDownThresholdReset(String resetStatus, SoftAssertions softly) {
		SolaceHealthSessionProperties properties = new SolaceHealthSessionProperties();
		properties.setReconnectAttemptsUntilDown(1L);
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(properties);

		healthIndicator.reconnecting(null, 0, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null, 0, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);

		switch (resetStatus) {
			case "DOWN":
				healthIndicator.down(null, 0, null);
				break;
			case "UP":
				healthIndicator.up();
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + resetStatus);
		}

		healthIndicator.reconnecting(null, 0, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null, 0, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
	}
}
