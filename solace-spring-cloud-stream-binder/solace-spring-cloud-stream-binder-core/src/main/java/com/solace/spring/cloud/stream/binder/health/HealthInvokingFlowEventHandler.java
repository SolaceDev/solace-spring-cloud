package com.solace.spring.cloud.stream.binder.health;

import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HealthInvokingFlowEventHandler implements FlowEventHandler {
	private final SolaceFlowHealthIndicator solaceFlowHealthIndicator;
	private static final Log logger = LogFactory.getLog(HealthInvokingFlowEventHandler.class);

	public HealthInvokingFlowEventHandler(SolaceFlowHealthIndicator solaceFlowHealthIndicator) {
		this.solaceFlowHealthIndicator = solaceFlowHealthIndicator;
	}

	@Override
	public void handleEvent(Object source, FlowEventArgs flowEventArgs) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("(%s): Received flow event %s.", source, flowEventArgs));
		}
		if (flowEventArgs.getEvent() != null) {
			switch (flowEventArgs.getEvent()) {
				case FLOW_DOWN:
					solaceFlowHealthIndicator.down(source, flowEventArgs);
					break;
				case FLOW_RECONNECTING:
					solaceFlowHealthIndicator.reconnecting(source, flowEventArgs);
					break;
				case FLOW_UP:
				case FLOW_RECONNECTED:
					solaceFlowHealthIndicator.up(source, flowEventArgs);
					break;
			}
		}
	}

	public void connected() {
		solaceFlowHealthIndicator.up(null, null);
	}
}
