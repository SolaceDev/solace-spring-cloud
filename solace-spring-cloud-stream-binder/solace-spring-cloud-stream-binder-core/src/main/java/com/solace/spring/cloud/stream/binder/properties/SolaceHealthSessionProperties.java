package com.solace.spring.cloud.stream.binder.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;

@ConfigurationProperties("solace.health-check.session")
@Validated
public class SolaceHealthSessionProperties {
	/**
	 * <p>The number of session reconnect attempts until the health goes {@code DOWN}.
	 * This will happen regardless if the underlying session is actually still reconnecting.
	 * Setting this to {@code 0} will disable this feature.</p>
	 *
	 * <p>This feature operates independently of the PubSub+ session reconnect feature.
	 * Meaning that if PubSub+ session reconnect is configured to retry less than the value given to this property,
	 * then this feature effectively does nothing.</p>
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
