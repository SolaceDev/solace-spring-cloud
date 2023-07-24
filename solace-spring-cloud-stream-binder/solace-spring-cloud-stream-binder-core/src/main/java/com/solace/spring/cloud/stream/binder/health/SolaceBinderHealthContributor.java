package com.solace.spring.cloud.stream.binder.health;

import org.jetbrains.annotations.NotNull;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SolaceBinderHealthContributor implements CompositeHealthContributor {
	private SolaceSessionHealthIndicator solaceSessionHealthIndicator;
	private SolaceBindingsHealthContributor solaceBindingsHealthContributor;

	private static final String SESSION_KEY = "session";
	private static final String BINDINGS_KEY = "bindings";

	public SolaceBinderHealthContributor(SolaceSessionHealthIndicator solaceSessionHealthIndicator,
										 SolaceBindingsHealthContributor solaceBindingsHealthContributor) {
		this.solaceSessionHealthIndicator = solaceSessionHealthIndicator;
		this.solaceBindingsHealthContributor = solaceBindingsHealthContributor;
	}

	@Override
	public HealthContributor getContributor(String name) {
		switch (name) {
			case SESSION_KEY:
				return solaceSessionHealthIndicator;
			case BINDINGS_KEY:
				return solaceBindingsHealthContributor;
			default:
				return null;
		}
	}

	public SolaceSessionHealthIndicator getSolaceSessionHealthIndicator() {
		return solaceSessionHealthIndicator;
	}

	public SolaceBindingsHealthContributor getSolaceBindingsHealthContributor() {
		return solaceBindingsHealthContributor;
	}

	@NotNull
	@Override
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		List<NamedContributor<HealthContributor>> contributors = new ArrayList<>();
		contributors.add(NamedContributor.of(SESSION_KEY, solaceSessionHealthIndicator));
		contributors.add(NamedContributor.of(BINDINGS_KEY, solaceBindingsHealthContributor));
		return contributors.iterator();
	}
}
