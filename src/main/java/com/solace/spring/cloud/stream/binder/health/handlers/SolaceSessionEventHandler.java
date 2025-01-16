package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solace.spring.cloud.stream.binder.oauth.DefaultSolaceOAuth2SessionEventHandler;
import com.solace.spring.cloud.stream.binder.oauth.SolaceSessionOAuth2TokenProvider;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

@Slf4j
public class SolaceSessionEventHandler extends DefaultSolaceOAuth2SessionEventHandler {
    private final SessionHealthIndicator sessionHealthIndicator;

    public SolaceSessionEventHandler(JCSMPProperties jcsmpProperties,
                                     @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider,
                                     SessionHealthIndicator sessionHealthIndicator) {
        super(jcsmpProperties, solaceSessionOAuth2TokenProvider);
        this.sessionHealthIndicator = sessionHealthIndicator;
    }

    @Override
    public void handleEvent(SessionEventArgs eventArgs) {
        log.debug("Received Solace JCSMP Session event [{}]", eventArgs);
        super.handleEvent(eventArgs);
        switch (eventArgs.getEvent()) {
            case RECONNECTED -> this.sessionHealthIndicator.up();
            case DOWN_ERROR -> this.sessionHealthIndicator.down(eventArgs);
            case RECONNECTING -> this.sessionHealthIndicator.reconnecting(eventArgs);
        }
    }

    public void setSessionHealthUp() {
        this.sessionHealthIndicator.up();
    }
}
