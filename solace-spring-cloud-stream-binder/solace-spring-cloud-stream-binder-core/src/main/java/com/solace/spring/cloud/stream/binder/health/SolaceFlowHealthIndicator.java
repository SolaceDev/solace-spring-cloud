package com.solace.spring.cloud.stream.binder.health;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthFlowProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class SolaceFlowHealthIndicator implements HealthIndicator {
	private static final String STATUS_RECONNECTING = "RECONNECTING";
	private static final String INFO = "info";
	private static final String RESPONSE_CODE = "responseCode";

	private final SolaceHealthFlowProperties solaceHealthFlowProperties;
	private final AtomicReference<Health> health = new AtomicReference<>(Health.down().build());
	private final AtomicLong reconnectCount = new AtomicLong(0);
	private final ReentrantLock writeLock = new ReentrantLock();

	private static final Log logger = LogFactory.getLog(SolaceFlowHealthIndicator.class);

	public SolaceFlowHealthIndicator(SolaceHealthFlowProperties solaceHealthFlowProperties) {
		this.solaceHealthFlowProperties = solaceHealthFlowProperties;
	}

	public void up(@Nullable Object source, @Nullable FlowEventArgs flowEventArgs) {
		writeLock.lock();
		try {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Solace flow status is %s", Status.UP));
			}
			if (logger.isTraceEnabled()) {
				logger.trace("Reset reconnect count");
			}
			reconnectCount.set(0);
			health.set(buildHealthUp(buildHealth(Health.up(), source, flowEventArgs), source, flowEventArgs).build());
		} finally {
			writeLock.unlock();
		}
	}

	public void reconnecting(@Nullable Object source, @Nullable FlowEventArgs flowEventArgs) {
		writeLock.lock();
		try {
			long reconnectAttempt = reconnectCount.incrementAndGet();
			if (Optional.of(solaceHealthFlowProperties.getReconnectAttemptsUntilDown())
					.filter(maxReconnectAttempts -> maxReconnectAttempts > 0)
					.filter(maxReconnectAttempts -> reconnectAttempt > maxReconnectAttempts)
					.isPresent()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Solace flow reconnect attempt %s > %s, changing state to down",
							reconnectAttempt, solaceHealthFlowProperties.getReconnectAttemptsUntilDown()));
				}
				down(source, flowEventArgs, false);
				return;
			}

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Solace flow status is %s (attempt %s)", STATUS_RECONNECTING,
						reconnectAttempt));
			}
			health.set(buildHealthReconnecting(buildHealth(Health.status(STATUS_RECONNECTING),
					source, flowEventArgs), source, flowEventArgs).build());
		} finally {
			writeLock.unlock();
		}
	}

	public void down(@Nullable Object source, @Nullable FlowEventArgs flowEventArgs) {
		down(source, flowEventArgs, true);
	}

	private void down(@Nullable Object source,
					  @Nullable FlowEventArgs flowEventArgs,
					  boolean resetReconnectCount) {
		writeLock.lock();
		try {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Solace flow status is %s", Status.DOWN));
			}
			if (resetReconnectCount) {
				if (logger.isTraceEnabled()) {
					logger.trace("Reset reconnect count");
				}
				reconnectCount.set(0);
			}
			health.set(buildHealthDown(buildHealth(Health.down(), source, flowEventArgs), source, flowEventArgs)
					.build());
		} finally {
			writeLock.unlock();
		}
	}

	protected Health.Builder buildHealth(Health.Builder builder,
										 @Nullable Object source,
										 @Nullable FlowEventArgs flowEventArgs) {
		return builder;
	}

	protected Health.Builder buildHealthUp(Health.Builder builder,
										   @Nullable Object source,
										   @Nullable FlowEventArgs flowEventArgs) {
		return builder;
	}

	protected Health.Builder buildHealthReconnecting(Health.Builder builder,
													 @Nullable Object source,
													 @Nullable FlowEventArgs flowEventArgs) {
		return addFlowEventDetails(builder, flowEventArgs);
	}

	protected Health.Builder buildHealthDown(Health.Builder builder,
											 @Nullable Object source,
											 @Nullable FlowEventArgs flowEventArgs) {
		return addFlowEventDetails(builder, flowEventArgs);
	}

	private Health.Builder addFlowEventDetails(Health.Builder builder,
											   @Nullable FlowEventArgs flowEventArgs) {
		if (flowEventArgs == null) {
			return builder;
		}

		Optional.ofNullable(flowEventArgs.getException())
				.ifPresent(builder::withException);
		Optional.of(flowEventArgs.getResponseCode())
				.filter(c -> c != 0)
				.ifPresent(c -> builder.withDetail(RESPONSE_CODE, c));
		Optional.ofNullable(flowEventArgs.getInfo())
				.filter(StringUtils::isNotBlank)
				.ifPresent(info -> builder.withDetail(INFO, info));
		return builder;
	}

	@Override
	public Health health() {
		return health.get();
	}
}
