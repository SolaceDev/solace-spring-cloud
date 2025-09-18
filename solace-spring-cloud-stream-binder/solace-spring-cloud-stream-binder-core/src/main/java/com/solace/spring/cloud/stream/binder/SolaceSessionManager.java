package com.solace.spring.cloud.stream.binder;

import static com.solacesystems.jcsmp.XMLMessage.Outcome.ACCEPTED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.FAILED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.REJECTED;

import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import com.solacesystems.jcsmp.impl.client.ClientInfoProvider;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;

public class SolaceSessionManager implements SolaceSessionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolaceSessionManager.class);

  private final JCSMPProperties jcsmpProperties;
  private final ClientInfoProvider clientInfoProvider;
  private final SolaceSessionEventHandler solaceSessionEventHandler;
  private final SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider;
  private final boolean failOnStartup;

  private volatile JCSMPSession jcsmpSession;
  private volatile Context context;
  private final Object sessionLock = new Object();

  public SolaceSessionManager(JCSMPProperties jcsmpProperties,
      @Nullable ClientInfoProvider clientInfoProvider,
      @Nullable SolaceSessionEventHandler eventHandler,
      @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider,
      @Value("${solace.session.failOnStartup:true}") boolean failOnStartup) {
    this.jcsmpProperties = jcsmpProperties;
    this.clientInfoProvider = clientInfoProvider;
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

  private void createSessionIfNeeded() {
    // Double-check pattern with consistent synchronization
    if (jcsmpSession != null && !jcsmpSession.isClosed()) {
      return;
    }

    synchronized (sessionLock) {
      // Double-check pattern
      if (jcsmpSession != null && !jcsmpSession.isClosed()) {
        return;
      }

      JCSMPProperties solaceJcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
      if (this.clientInfoProvider != null) {
        solaceJcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, clientInfoProvider);
      }

      try {
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
          if (jcsmpSession instanceof JCSMPBasicSession session
              && !session.isRequiredSettlementCapable(Set.of(ACCEPTED, FAILED, REJECTED))) {
            LOGGER.warn(
                "The connected Solace PubSub+ Broker is not compatible. It doesn't support message NACK capability. Consumer bindings will fail to start.");
          }

          LOGGER.info("Successfully created and connected Solace JCSMP session");
        } catch (JCSMPException exc) {
          //Throw exception to prevent application from starting if configured to do so (currently the default)
          if (failOnStartup) {
            throw exc;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to initialize Solace JCSMP session", e);
        if (context != null) {
          context.destroy();
        }

        //Throw runtime exception to prevent application from starting
        throw new SolaceSessionException("Failed to initialize Solace JCSMP session", e);
      }
    }
  }

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
   * a RuntimeException for Solace session failures.
   */
  public static class SolaceSessionException extends RuntimeException {

    public SolaceSessionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}