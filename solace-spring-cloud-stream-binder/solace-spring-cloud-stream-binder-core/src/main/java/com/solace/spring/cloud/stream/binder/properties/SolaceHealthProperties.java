package com.solace.spring.cloud.stream.binder.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("solace.health-check")
@Validated
public class SolaceHealthProperties {
	private SolaceHealthSessionProperties session = new SolaceHealthSessionProperties();

	public SolaceHealthSessionProperties getSession() {
		return session;
	}

	public void setSession(SolaceHealthSessionProperties session) {
		this.session = session;
	}
}
