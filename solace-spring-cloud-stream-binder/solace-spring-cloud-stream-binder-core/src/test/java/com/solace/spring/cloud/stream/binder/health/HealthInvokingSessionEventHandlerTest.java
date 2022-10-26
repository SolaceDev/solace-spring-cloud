package com.solace.spring.cloud.stream.binder.health;

import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class HealthInvokingSessionEventHandlerTest {
	@Test
	public void testConnected(@Mock SolaceSessionHealthIndicator healthIndicator) {
		HealthInvokingSessionEventHandler sessionEventHandler = new HealthInvokingSessionEventHandler(healthIndicator);
		sessionEventHandler.connected();
		Mockito.verify(healthIndicator, Mockito.times(1)).up(null);
		Mockito.verifyNoMoreInteractions(healthIndicator);
	}

	@ParameterizedTest(name = "[{index}] event={0}")
	@EnumSource(SessionEvent.class)
	public void testHandleEvent(SessionEvent event,
								@Mock SessionEventArgs eventArgs,
								@Mock SolaceSessionHealthIndicator healthIndicator) {
		Mockito.when(eventArgs.getEvent()).thenReturn(event);
		HealthInvokingSessionEventHandler sessionEventHandler = new HealthInvokingSessionEventHandler(healthIndicator);
		sessionEventHandler.handleEvent(eventArgs);

		switch (event) {
			case DOWN_ERROR:
				Mockito.verify(healthIndicator, Mockito.times(1)).down(eventArgs);
				break;
			case RECONNECTING:
				Mockito.verify(healthIndicator, Mockito.times(1)).reconnecting(eventArgs);
				break;
			case RECONNECTED:
				Mockito.verify(healthIndicator, Mockito.times(1)).up(eventArgs);
				break;
			default:
				Mockito.verifyNoInteractions(healthIndicator);
		}

		Mockito.verifyNoMoreInteractions(healthIndicator);
	}
}
