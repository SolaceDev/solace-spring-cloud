package com.solace.spring.cloud.stream.binder.properties;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;

@Validated
public class SolaceHealthFlowProperties {
	@Min(0)
	private long reconnectAttemptsUntilDown = 0;

	public long getReconnectAttemptsUntilDown() {
		return reconnectAttemptsUntilDown;
	}

	public void setReconnectAttemptsUntilDown(Long reconnectAttemptsUntilDown) {
		this.reconnectAttemptsUntilDown = reconnectAttemptsUntilDown;
	}
}
