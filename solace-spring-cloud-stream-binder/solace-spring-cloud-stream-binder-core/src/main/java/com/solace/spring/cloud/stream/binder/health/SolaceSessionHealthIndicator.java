package com.solace.spring.cloud.stream.binder.health;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthSessionProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class SolaceSessionHealthIndicator implements HealthIndicator {

    private static final String STATUS_RECONNECTING = "RECONNECTING";
    private static final String INFO = "info";
    private static final String RESPONSE_CODE = "responseCode";

    private final SolaceHealthSessionProperties solaceHealthSessionProperties;
    private volatile Health health;
    private final AtomicLong reconnectCount = new AtomicLong(0);
    private final ReentrantLock writeLock = new ReentrantLock();

    private static final Log logger = LogFactory.getLog(SolaceSessionHealthIndicator.class);

    public SolaceSessionHealthIndicator(SolaceHealthSessionProperties solaceHealthSessionProperties) {
        this.solaceHealthSessionProperties = solaceHealthSessionProperties;
    }

    public void up(@Nullable SessionEventArgs sessionEventArgs) {
        writeLock.lock();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Solace session status is %s", Status.UP));
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Reset reconnect count");
            }
            reconnectCount.set(0);
            health = buildHealthUp(buildHealth(Health.up(), sessionEventArgs), sessionEventArgs).build();
        } finally {
            writeLock.unlock();
        }
    }

    public void reconnecting(@Nullable SessionEventArgs sessionEventArgs) {
        writeLock.lock();
        try {
            long reconnectAttempt = reconnectCount.incrementAndGet();
            if (Optional.of(solaceHealthSessionProperties.getReconnectAttemptsUntilDown())
                    .filter(maxReconnectAttempts -> maxReconnectAttempts > 0)
                    .filter(maxReconnectAttempts -> reconnectAttempt > maxReconnectAttempts)
                    .isPresent()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Solace session reconnect attempt %s > %s, changing state to down",
                            reconnectAttempt, solaceHealthSessionProperties.getReconnectAttemptsUntilDown()));
                }
                down(sessionEventArgs, false);
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Solace session status is %s (attempt %s)", STATUS_RECONNECTING,
                        reconnectAttempt));
            }
            health = buildHealthReconnecting(buildHealth(Health.status(STATUS_RECONNECTING),
                    sessionEventArgs), sessionEventArgs).build();
        } finally {
            writeLock.unlock();
        }
    }

    public void down(@Nullable SessionEventArgs sessionEventArgs) {
        down(sessionEventArgs, true);
    }

    private void down(@Nullable SessionEventArgs sessionEventArgs, boolean resetReconnectCount) {
        writeLock.lock();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Solace session status is %s", Status.DOWN));
            }
            if (resetReconnectCount) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Reset reconnect count");
                }
                reconnectCount.set(0);
            }
            health = buildHealthDown(buildHealth(Health.down(), sessionEventArgs), sessionEventArgs)
                    .build();
        } finally {
            writeLock.unlock();
        }
    }

    protected Health.Builder buildHealth(Health.Builder builder, @Nullable SessionEventArgs sessionEventArgs) {
        return builder;
    }

    protected Health.Builder buildHealthUp(Health.Builder builder, @Nullable SessionEventArgs sessionEventArgs) {
        return builder;
    }

    protected Health.Builder buildHealthReconnecting(Health.Builder builder,
                                                     @Nullable SessionEventArgs sessionEventArgs) {
        return addSessionEventDetails(builder, sessionEventArgs);
    }

    protected Health.Builder buildHealthDown(Health.Builder builder, @Nullable SessionEventArgs sessionEventArgs) {
        return addSessionEventDetails(builder, sessionEventArgs);
    }

    private Health.Builder addSessionEventDetails(Health.Builder builder, @Nullable SessionEventArgs sessionEventArgs) {
        if (sessionEventArgs == null) {
            return builder;
        }

        Optional.ofNullable(sessionEventArgs.getException())
                .ifPresent(builder::withException);
        Optional.of(sessionEventArgs.getResponseCode())
                .filter(c -> c != 0)
                .ifPresent(c -> builder.withDetail(RESPONSE_CODE, c));
        Optional.ofNullable(sessionEventArgs.getInfo())
                .filter(StringUtils::isNotBlank)
                .ifPresent(info -> builder.withDetail(INFO, info));
        return builder;
    }

    @Override
    public Health health() {
        return health;
    }
}
