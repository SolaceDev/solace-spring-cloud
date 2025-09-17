package com.solace.spring.cloud.stream.binder;

import static com.solacesystems.jcsmp.XMLMessage.Outcome.ACCEPTED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.FAILED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.REJECTED;

import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import jakarta.annotation.PreDestroy;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;

/**
 * Manages the lifecycle of the Solace JCSMP session and context. Provides lazy initialization and
 * handles connection failures gracefully.
 */
public class SolaceSessionManager implements SolaceSessionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolaceSessionManager.class);

  private final JCSMPProperties jcsmpProperties;
  private final SolaceSessionEventHandler solaceSessionEventHandler;
  private final SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider;
  private final boolean failOnStartup;

  private JCSMPSession jcsmpSession;
  private Context context;
  private final Object sessionLock = new Object();

  public SolaceSessionManager(JCSMPProperties jcsmpProperties,
      @Nullable SolaceSessionEventHandler eventHandler,
      @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider,
      @Value("${solace.session.failOnStartup:true}") boolean failOnStartup) {
    this.jcsmpProperties = jcsmpProperties;
    this.solaceSessionEventHandler = eventHandler;
    this.solaceSessionOAuth2TokenProvider = solaceSessionOAuth2TokenProvider;
    this.failOnStartup = failOnStartup;
  }

  @Override
  public JCSMPSession getSession() {
    if (jcsmpSession == null || jcsmpSession.isClosed()) {
      createSessionIfNeeded();
    }
    return jcsmpSession;
  }

  @Override
  @Nullable
  public Context getContext() {
    if (solaceSessionEventHandler != null && (context == null || jcsmpSession == null
        || jcsmpSession.isClosed())) {
      createSessionIfNeeded();
    }
    return context;
  }

  @Override
  public boolean isConnected() {
    return jcsmpSession != null && !jcsmpSession.isClosed();
  }

  @Override
  public String getSessionInfo() {
    JCSMPSession session = jcsmpSession;
    if (session != null && !session.isClosed()) {
      return "Connected - " + session.getSessionName();
    }
    return "Not connected";
  }

  /**
   * Create the session and context if they don't exist or are closed. Thread-safe method that
   * handles lazy initialization.
   */
  private synchronized void createSessionIfNeeded() {
    // Double-check pattern
    if (jcsmpSession != null && !jcsmpSession.isClosed()) {
      return;
    }

    LOGGER.info("Creating Solace JCSMP session");

    JCSMPProperties solaceJcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
    //solaceJcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, new SolaceBinderClientInfoProvider());

    try {
      SpringJCSMPFactory springJCSMPFactory = new SpringJCSMPFactory(solaceJcsmpProperties,
          solaceSessionOAuth2TokenProvider);

      if (solaceSessionEventHandler != null) {
        LOGGER.debug("Registering Solace Session Event handler on session");
        context = springJCSMPFactory.createContext(new ContextProperties());
        jcsmpSession = springJCSMPFactory.createSession(context, solaceSessionEventHandler);
      } else {
        jcsmpSession = springJCSMPFactory.createSession();
      }

      if (solaceSessionEventHandler != null) {
        solaceSessionEventHandler.setSessionHealthReconnecting();
      }

      LOGGER.info("Connecting JCSMP session {}", jcsmpSession.getSessionName());
      jcsmpSession.connect();

      if (solaceSessionEventHandler != null) {
        solaceSessionEventHandler.setSessionHealthUp();
      }

      // Check broker compatibility
      if (jcsmpSession instanceof JCSMPBasicSession session &&
          !session.isRequiredSettlementCapable(Set.of(ACCEPTED, FAILED, REJECTED))) {
        LOGGER.warn(
            "The connected Solace PubSub+ Broker is not compatible. It doesn't support message NACK capability. Consumer bindings will fail to start.");
      }

      LOGGER.info("Successfully created and connected Solace JCSMP session");

    } catch (Exception e) {
      if (solaceSessionEventHandler != null) {
        solaceSessionEventHandler.setSessionHealthDown();
      }
      // Clean up on failure
      if (context != null) {
        try {
          context.destroy();
        } catch (Exception destroyException) {
          LOGGER.warn("Failed to destroy context during cleanup", destroyException);
        }
        context = null;
      }
      jcsmpSession = null;

      LOGGER.error("Failed to initialize Solace JCSMP session", e);
      throw new SolaceSessionException("Failed to initialize Solace JCSMP session", e);
    }
  }

  /**
   * Close the session and context gracefully.
   */
  @PreDestroy
  public void close() {
    synchronized (sessionLock) {
      if (jcsmpSession != null) {
        try {
          LOGGER.info("Closing Solace JCSMP session");
          jcsmpSession.closeSession();
        } catch (Exception e) {
          LOGGER.warn("Error closing JCSMP session", e);
        } finally {
          jcsmpSession = null;
        }
      }

      if (context != null) {
        try {
          context.destroy();
        } catch (Exception e) {
          LOGGER.warn("Error destroying JCSMP context", e);
        } finally {
          context = null;
        }
      }
    }
  }

  /**
   * Custom exception for Solace session failures.
   */
  public static class SolaceSessionException extends RuntimeException {

    public SolaceSessionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
