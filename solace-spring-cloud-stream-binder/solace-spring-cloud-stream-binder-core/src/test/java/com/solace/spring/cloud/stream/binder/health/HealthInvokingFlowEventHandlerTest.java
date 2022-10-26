package com.solace.spring.cloud.stream.binder.health;

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowReceiver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class HealthInvokingFlowEventHandlerTest {
	@Test
	public void testConnected(@Mock SolaceFlowHealthIndicator healthIndicator) {
		HealthInvokingFlowEventHandler flowEventHandler = new HealthInvokingFlowEventHandler(healthIndicator);
		flowEventHandler.connected();
		Mockito.verify(healthIndicator, Mockito.times(1)).up(null, null);
		Mockito.verifyNoMoreInteractions(healthIndicator);
	}

	@ParameterizedTest(name = "[{index}] event={0}")
	@EnumSource(FlowEvent.class)
	public void testHandleEvent(FlowEvent event,
								@Mock FlowReceiver source,
								@Mock SolaceFlowHealthIndicator healthIndicator) {
		FlowEventArgs eventArgs = new FlowEventArgs(event, null, null, 0);
		HealthInvokingFlowEventHandler flowEventHandler = new HealthInvokingFlowEventHandler(healthIndicator);
		flowEventHandler.handleEvent(source, eventArgs);

		switch (event) {
			case FLOW_DOWN:
				Mockito.verify(healthIndicator, Mockito.times(1)).down(null, eventArgs);
				break;
			case FLOW_RECONNECTING:
				Mockito.verify(healthIndicator, Mockito.times(1)).reconnecting(null, eventArgs);
				break;
			case FLOW_UP:
			case FLOW_RECONNECTED:
				Mockito.verify(healthIndicator, Mockito.times(1)).up(null, eventArgs);
				break;
			default:
				Mockito.verifyNoInteractions(healthIndicator);
		}

		Mockito.verifyNoMoreInteractions(healthIndicator);
	}
}
