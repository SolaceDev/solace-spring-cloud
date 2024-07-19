package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SolaceFlowEventHandler implements FlowEventHandler {

    private final XMLMessageMapper xmlMessageMapper;
    private final String flowReceiverContainerId;

    public SolaceFlowEventHandler(XMLMessageMapper xmlMessageMapper, String flowReceiverContainerId) {
        this.xmlMessageMapper = xmlMessageMapper;
        this.flowReceiverContainerId = flowReceiverContainerId;
    }

    @Override
    public void handleEvent(Object source, FlowEventArgs flowEventArgs) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("(%s): Received Solace Flow event [%s].", source, flowEventArgs));
        }

        if (flowEventArgs.getEvent() == FlowEvent.FLOW_RECONNECTED && xmlMessageMapper != null) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Received flow event %s for flow receiver container %s. Will clear ignored properties.",
                        flowEventArgs.getEvent().name(), flowReceiverContainerId));
            }
            xmlMessageMapper.resetIgnoredProperties(flowReceiverContainerId);
        }
    }

}
