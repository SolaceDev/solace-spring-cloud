package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthSessionProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.impl.SessionEventArgsImpl;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SolaceSessionHealthIndicatorTest {
	@Test
	public void testInitialHealth() {
		assertNull(new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties()).health());
	}

	@Test
	public void testUp(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.up(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.down(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		healthIndicator.reconnecting(null);
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
			healthIndicator.reconnecting(null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		}

		for (int i = 0; i < 3; i++) {
			healthIndicator.reconnecting(null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
		}
	}

	@ParameterizedTest(name = "[{index}] resetStatus={0}")
	@ValueSource(strings = {"DOWN", "UP"})
	public void testReconnectingDownThresholdReset(String resetStatus, SoftAssertions softly) {
		SolaceHealthSessionProperties properties = new SolaceHealthSessionProperties();
		properties.setReconnectAttemptsUntilDown(1L);
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(properties);

		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);

		switch (resetStatus) {
			case "DOWN":
				healthIndicator.down(null);
				break;
			case "UP":
				healthIndicator.up(null);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + resetStatus);
		}

		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
	}

	@CartesianTest(name = "[{index}] status={0} responseCode={1} info={2}")
	public void testDetails(@CartesianTest.Values(strings = {"DOWN", "RECONNECTING", "UP"}) String status,
							@CartesianTest.Values(booleans = {false, true}) boolean withException,
							@CartesianTest.Values(ints = {-1, 0, 1}) int responseCode,
							@CartesianTest.Values(strings = {"", "some-info"}) String info,
							SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		Exception healthException = withException ? new Exception("test") : null;
		SessionEventArgs sessionEventArgs = new SessionEventArgsImpl(null, info, healthException, responseCode);
		switch (status) {
			case "DOWN":
				healthIndicator.down(sessionEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(sessionEventArgs);
				break;
			case "UP":
				healthIndicator.up(sessionEventArgs);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.health();

		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));

		if (withException && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.isNotEmpty()
					.extractingByKey("error")
					.isEqualTo(healthException.getClass().getName() + ": " + healthException.getMessage());
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("error");
		}

		if (responseCode != 0 && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.extractingByKey("responseCode")
					.isEqualTo(responseCode);
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("responseCode");
		}

		if (!info.isEmpty() && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.extractingByKey("info")
					.isEqualTo(info);
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("info");
		}
	}

	@ParameterizedTest(name = "[{index}] status={0}")
	@ValueSource(strings = {"DOWN", "RECONNECTING", "UP"})
	public void testWithoutDetails(String status, SoftAssertions softly) {
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties());
		SessionEventArgs sessionEventArgs = new SessionEventArgsImpl(null, "some-info", new RuntimeException("test"), 1);
		switch (status) {
			case "DOWN":
				healthIndicator.down(sessionEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(sessionEventArgs);
				break;
			case "UP":
				healthIndicator.up(sessionEventArgs);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.getHealth(false);
		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@ParameterizedTest(name = "[{index}] status={0}")
	@ValueSource(strings = {"DOWN", "RECONNECTING", "UP"})
	public void testDetailsOverride(String status, SoftAssertions softly) {
		String customDetail = "custom-detail";
		String overriddenDetail = "overridden-detail";
		String statusCustomDetail = "status-custom-detail";
		SessionEventArgs sessionEventArgs = new SessionEventArgsImpl(null, "some-info", new RuntimeException("test"), 1);
		SolaceSessionHealthIndicator healthIndicator = new SolaceSessionHealthIndicator(new SolaceHealthSessionProperties()) {
			@Override
			protected Health.Builder buildHealth(Health.Builder builder, SessionEventArgs sessionEventArgs) {
				return builder.withDetail(customDetail, customDetail).withDetail(overriddenDetail, overriddenDetail);
			}

			@Override
			protected Health.Builder buildHealthUp(Health.Builder builder, SessionEventArgs sessionEventArgs) {
				softly.assertThat(sessionEventArgs).isEqualTo(sessionEventArgs);
				return builder.withDetail(statusCustomDetail, "UP").withDetail(overriddenDetail, "UP");
			}

			@Override
			protected Health.Builder buildHealthReconnecting(Health.Builder builder, SessionEventArgs sessionEventArgs) {
				softly.assertThat(sessionEventArgs).isEqualTo(sessionEventArgs);
				return builder.withDetail(statusCustomDetail, "RECONNECTING").withDetail(overriddenDetail, "RECONNECTING");
			}

			@Override
			protected Health.Builder buildHealthDown(Health.Builder builder, SessionEventArgs sessionEventArgs) {
				softly.assertThat(sessionEventArgs).isEqualTo(sessionEventArgs);
				return builder.withDetail(statusCustomDetail, "DOWN").withDetail(overriddenDetail, "DOWN");
			}
		};

		switch (status) {
			case "DOWN":
				healthIndicator.down(sessionEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(sessionEventArgs);
				break;
			case "UP":
				healthIndicator.up(sessionEventArgs);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}

		softly.assertThat(healthIndicator.health().getDetails())
				.isNotEmpty()
				.hasSize(3)
				.satisfies(d -> {
					assertThat(d.get(customDetail)).isEqualTo(customDetail);
					assertThat(d.get(overriddenDetail)).isEqualTo(status);
					assertThat(d.get(statusCustomDetail)).isEqualTo(status);
				});
	}
}
