package com.solace.spring.cloud.stream.binder;

import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.JCSMPSession;
import org.springframework.lang.Nullable;

/**
 * Provider interface for accessing Solace JCSMP session and context.
 * This allows for lazy initialization and deferred session creation.
 */
public interface SolaceSessionProvider {

    /**
     * Get the JCSMP session, creating it lazily if needed.
     * @return the JCSMP session
     * @throws RuntimeException if session creation fails
     */
    JCSMPSession getSession();

    /**
     * Get the JCSMP context, creating it lazily if needed.
     * @return the JCSMP context, or null if no event handler is configured
     */
    @Nullable
    Context getContext();

    /**
     * Check if the session is currently connected.
     * @return true if session exists and is connected, false otherwise
     */
    boolean isConnected();

    /**
     * Get session information for debugging/monitoring purposes.
     * @return session info string
     */
    String getSessionInfo();
}
