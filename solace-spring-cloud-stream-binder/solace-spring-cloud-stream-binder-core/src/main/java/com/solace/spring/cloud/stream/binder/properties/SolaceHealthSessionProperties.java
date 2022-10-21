package com.solace.spring.cloud.stream.binder.properties;

import javax.validation.constraints.Min;

public class SolaceHealthSessionProperties {
	/**
	 * The number of session reconnect attempts until the health goes {@code DOWN}.
	 * This will happen regardless if the underlying session is actually still reconnecting.
	 * Set this to {@code 0} will disable this feature.
	 * This feature operates independently of the PubSub+ session reconnect feature.
	 */
	@Min(0)
	private long reconnectAttemptsUntilDown = 0;

	public long getReconnectAttemptsUntilDown() {
		return reconnectAttemptsUntilDown;
	}

	public void setReconnectAttemptsUntilDown(Long reconnectAttemptsUntilDown) {
		this.reconnectAttemptsUntilDown = reconnectAttemptsUntilDown;
	}
}
