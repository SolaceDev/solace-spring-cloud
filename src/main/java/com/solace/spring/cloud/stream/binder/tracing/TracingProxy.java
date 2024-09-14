package com.solace.spring.cloud.stream.binder.tracing;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

/**
 * <p>Proxy class for the Solace binder to access tracing components.
 * Always use this instead of directly using tracing components in Solace binder code.</p>
 * <p>Allows for the Solace binder to still function correctly without micrometer on the classpath.</p>
 */
@RequiredArgsConstructor
public class TracingProxy {
    public static final String TRACING_HEADER_KEY = "TRACING_HEADER_KEY";
    private final TracingImpl tracingImpl;

    public SDTMap getTracingHeader() {
        return tracingImpl.getTracingHeader();
    }

    public Consumer<Message<?>> wrapInTracingContext(SDTMap tracingHeader, Consumer<Message<?>> messageConsumer) {
        return this.tracingImpl.wrapInTracingContext(tracingHeader, messageConsumer);
    }
}
