package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthSessionProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class SolaceSessionHealthIndicator implements HealthIndicator {

    private static final String STATUS_RECONNECTING = "RECONNECTING";
    private static final String INFO = "info";
    private static final String RESPONSE_CODE = "responseCode";

    private final SolaceHealthSessionProperties solaceHealthSessionProperties;
    private volatile Health healthStatus;
    private final AtomicLong reconnectCount = new AtomicLong(0);
    private final ReentrantLock writeLock = new ReentrantLock();

    private static final Log logger = LogFactory.getLog(SolaceSessionHealthIndicator.class);

    public SolaceSessionHealthIndicator(SolaceHealthSessionProperties solaceHealthSessionProperties) {
        this.solaceHealthSessionProperties = solaceHealthSessionProperties;
    }

    public void up() {
        writeLock.lock();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Solace session status is %s", Status.UP));
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Reset reconnect count");
            }
            reconnectCount.set(0);
            healthStatus = Health.up().build();
        } finally {
            writeLock.unlock();
        }
    }

    public void reconnecting(Exception exception, int responseCode, String info) {
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
                down(exception, responseCode, info, false);
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Solace session status is %s (attempt %s)", STATUS_RECONNECTING,
                        reconnectAttempt));
            }
            healthStatus = addSessionEventDetails(Health.status(STATUS_RECONNECTING), exception, responseCode, info)
                    .build();
        } finally {
            writeLock.unlock();
        }
    }

    public void down(Exception exception, int responseCode, String info) {
        down(exception, responseCode, info, true);
    }

    private void down(Exception exception, int responseCode, String info, boolean resetReconnectCount) {
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
            healthStatus = addSessionEventDetails(Health.down(), exception, responseCode, info).build();
        } finally {
            writeLock.unlock();
        }
    }

    private Health.Builder addSessionEventDetails(Health.Builder builder,
                                                  Exception exception,
                                                  int responseCode,
                                                  String info) {
        if (exception != null) builder.withException(exception);
        if (responseCode != 0) builder.withDetail(RESPONSE_CODE, responseCode);
        if (info != null && !info.isEmpty()) builder.withDetail(INFO, info);
        return builder;
    }

    @Override
    public Health health() {
        return healthStatus;
    }
}
