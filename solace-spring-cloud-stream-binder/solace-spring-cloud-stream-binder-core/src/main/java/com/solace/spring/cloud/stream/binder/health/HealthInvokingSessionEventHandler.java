package com.solace.spring.cloud.stream.binder.health;

import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class HealthInvokingSessionEventHandler implements SessionEventHandler {

    private final SolaceSessionHealthIndicator solaceSessionHealthIndicator;
    private static final Log logger = LogFactory.getLog(HealthInvokingSessionEventHandler.class);

    public HealthInvokingSessionEventHandler(SolaceSessionHealthIndicator solaceSessionHealthIndicator) {
        this.solaceSessionHealthIndicator = solaceSessionHealthIndicator;
    }

    @Override
    public void handleEvent(SessionEventArgs sessionEvent) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Received Solace session event %s.", sessionEvent));
        }
        switch (sessionEvent.getEvent()) {
            case DOWN_ERROR:
                solaceSessionHealthIndicator.down(sessionEvent);
                break;
            case RECONNECTING:
                solaceSessionHealthIndicator.reconnecting(sessionEvent);
                break;
            case RECONNECTED:
                solaceSessionHealthIndicator.up(sessionEvent);
                break;
        }
    }

    public void connected() {
        solaceSessionHealthIndicator.up(null);
    }
}
