package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.health.HealthInvokingFlowEventHandler;
import com.solace.spring.cloud.stream.binder.health.SolaceFlowHealthIndicator;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolaceBinderFlowEventHandler extends HealthInvokingFlowEventHandler {

    private static final Log logger = LogFactory.getLog(SolaceBinderFlowEventHandler.class);
    private final XMLMessageMapper xmlMessageMapper;
    private final String flowReceiverContainerId;

    public SolaceBinderFlowEventHandler(XMLMessageMapper xmlMessageMapper,
                                        String flowReceiverContainerId,
                                        SolaceFlowHealthIndicator solaceFlowHealthIndicator) {
        super(solaceFlowHealthIndicator);
        this.xmlMessageMapper = xmlMessageMapper;
        this.flowReceiverContainerId = flowReceiverContainerId;
    }

    @Override
    public void handleEvent(Object o, FlowEventArgs flowEventArgs) {
        super.handleEvent(o, flowEventArgs);
        if (flowEventArgs.getEvent() == FlowEvent.FLOW_RECONNECTED && xmlMessageMapper != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Received flow event %s for flow receiver container %s. Will clear ignored properties.",
                        flowEventArgs.getEvent().name(), flowReceiverContainerId));
            }
            xmlMessageMapper.resetIgnoredProperties(flowReceiverContainerId);
        }
    }

}