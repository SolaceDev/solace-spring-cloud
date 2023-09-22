package com.solace.spring.cloud.stream.binder.health;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthFlowProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceFlowHealthIndicatorTest {
	@Test
	public void testInitialHealth() {
		assertThat(new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties()).health())
				.extracting(Health::getStatus)
				.isEqualTo(Status.DOWN);
	}

	@Test
	public void testUp(SoftAssertions softly) {
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties());
		healthIndicator.up(null, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties());
		healthIndicator.down(null, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties());
		healthIndicator.reconnecting(null, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(new Status("RECONNECTING"));
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@ParameterizedTest(name = "[{index}] reconnectAttemptsUntilDown={0}")
	@ValueSource(longs = {1, 10})
	public void testReconnectingDownThresholdReached(long reconnectAttemptsUntilDown, SoftAssertions softly) {
		SolaceHealthFlowProperties properties = new SolaceHealthFlowProperties();
		properties.setReconnectAttemptsUntilDown(reconnectAttemptsUntilDown);
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(properties);
		for (int i = 0; i < reconnectAttemptsUntilDown; i++) {
			healthIndicator.reconnecting(null, null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		}

		for (int i = 0; i < 3; i++) {
			healthIndicator.reconnecting(null, null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
		}
	}

	@ParameterizedTest(name = "[{index}] resetStatus={0}")
	@ValueSource(strings = {"DOWN", "UP"})
	public void testReconnectingDownThresholdReset(String resetStatus, SoftAssertions softly) {
		SolaceHealthFlowProperties properties = new SolaceHealthFlowProperties();
		properties.setReconnectAttemptsUntilDown(1L);
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(properties);

		healthIndicator.reconnecting(null, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);

		switch (resetStatus) {
			case "DOWN":
				healthIndicator.down(null, null);
				break;
			case "UP":
				healthIndicator.up(null, null);
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + resetStatus);
		}

		healthIndicator.reconnecting(null, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null, null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
	}

	@CartesianTest(name = "[{index}] status={0} responseCode={1} info={2}")
	public void testDetails(@CartesianTest.Values(strings = {"DOWN", "RECONNECTING", "UP"}) String status,
							@CartesianTest.Values(booleans = {false, true}) boolean withException,
							@CartesianTest.Values(ints = {-1, 0, 1}) int responseCode,
							@CartesianTest.Values(strings = {"", "some-info"}) String info,
							SoftAssertions softly) {
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties());
		Exception healthException = withException ? new Exception("test") : null;
		FlowEventArgs flowEventArgs = new FlowEventArgs(null, info, healthException, responseCode);
		switch (status) {
			case "DOWN":
				healthIndicator.down(null, flowEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(null, flowEventArgs);
				break;
			case "UP":
				healthIndicator.up(null, flowEventArgs);
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
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties());
		FlowEventArgs flowEventArgs = new FlowEventArgs(null, "some-info", new RuntimeException("test"), 1);
		switch (status) {
			case "DOWN":
				healthIndicator.down(null, flowEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(null, flowEventArgs);
				break;
			case "UP":
				healthIndicator.up(null, flowEventArgs);
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
		FlowEventArgs flowEventArgs = new FlowEventArgs(null, "some-info", new RuntimeException("test"), 1);
		SolaceFlowHealthIndicator healthIndicator = new SolaceFlowHealthIndicator(new SolaceHealthFlowProperties()) {
			@Override
			protected Health.Builder buildHealth(Health.Builder builder/*, Object source, FlowEventArgs flowEventArgs*/) {
				return builder.withDetail(customDetail, customDetail).withDetail(overriddenDetail, overriddenDetail);
			}

			@Override
			protected Health.Builder buildHealthUp(Health.Builder builder/*, Object source, FlowEventArgs flowEventArgs*/) {
				/*softly.assertThat(flowEventArgs).isEqualTo(flowEventArgs);*/
				return builder.withDetail(statusCustomDetail, "UP").withDetail(overriddenDetail, "UP");
			}

			@Override
			protected Health.Builder buildHealthReconnecting(Health.Builder builder, /*Object source,*/ FlowEventArgs flowEventArgs) {
				softly.assertThat(flowEventArgs).isEqualTo(flowEventArgs);
				return builder.withDetail(statusCustomDetail, "RECONNECTING").withDetail(overriddenDetail, "RECONNECTING");
			}

			@Override
			protected Health.Builder buildHealthDown(Health.Builder builder, /*Object source,*/ FlowEventArgs flowEventArgs) {
				softly.assertThat(flowEventArgs).isEqualTo(flowEventArgs);
				return builder.withDetail(statusCustomDetail, "DOWN").withDetail(overriddenDetail, "DOWN");
			}
		};

		switch (status) {
			case "DOWN":
				healthIndicator.down(null, flowEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(null, flowEventArgs);
				break;
			case "UP":
				healthIndicator.up(null, flowEventArgs);
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
