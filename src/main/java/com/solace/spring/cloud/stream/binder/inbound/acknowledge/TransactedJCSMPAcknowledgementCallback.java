package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.acks.AcknowledgmentCallback;

@Slf4j
class TransactedJCSMPAcknowledgementCallback implements AcknowledgmentCallback {
    private final TransactedSession transactedSession;
    private final ErrorQueueInfrastructure errorQueueInfrastructure;
    private final long creationThreadId = Thread.currentThread().getId();
    private boolean acknowledged = false;

    TransactedJCSMPAcknowledgementCallback(TransactedSession transactedSession,
                                           ErrorQueueInfrastructure errorQueueInfrastructure) {
        this.transactedSession = transactedSession;
        this.errorQueueInfrastructure = errorQueueInfrastructure;
    }

    @Override
    public void acknowledge(Status status) {
        if (acknowledged) {
            log.debug("transaction is already resolved");
            return;
        }

        if (creationThreadId != Thread.currentThread().getId()) {
            throw new UnsupportedOperationException("Transactions must be resolved on the message handler's thread");
        }

        try {
            switch (status) {
                case ACCEPT -> {
                    try {
                        transactedSession.commit();
                    } catch (JCSMPException e) {
                        if (!(e instanceof RollbackException)) {
                            try {
                                log.debug("Rolling back transaction");
                                transactedSession.rollback();
                            } catch (JCSMPException e1) {
                                e.addSuppressed(e1);
                            }
                        }

                        throw e;
                    }
                }
                case REJECT -> {
                    if (!republishToErrorQueue()) {
                        transactedSession.rollback();
                    }
                }
                case REQUEUE -> transactedSession.rollback();
            }
        } catch (Exception e) {
            throw new SolaceAcknowledgmentException("Failed to resolve transaction", e);
        }

        acknowledged = true;
    }

    /**
     * Send the message to the error queue and acknowledge the message.
     *
     * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not
     * defined.
     */
    private boolean republishToErrorQueue() {
        return false; //TODO
    }

    @Override
    public boolean isAcknowledged() {
        return acknowledged;
    }
}
