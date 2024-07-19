package com.solace.spring.cloud.stream.binder.test.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleJCSMPEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

    private final boolean logErrors;

    public SimpleJCSMPEventHandler() {
        this(true);
    }

    public SimpleJCSMPEventHandler(boolean logErrors) {
        this.logErrors = logErrors;
    }

    @Override
    public void responseReceivedEx(Object key) {
        log.debug("Got message with key: {}", key);
    }

    @Override
    public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
        if (logErrors) {
            log.error("Failed to receive message at {} with key {}", timestamp, key, e);
        } else {
            log.trace("Failed to receive message at {} with key {}", timestamp, key, e);
        }
    }
}
