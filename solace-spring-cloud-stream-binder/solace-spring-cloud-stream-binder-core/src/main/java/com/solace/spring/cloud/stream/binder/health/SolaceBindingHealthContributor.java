package com.solace.spring.cloud.stream.binder.health;

import org.jetbrains.annotations.NotNull;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class SolaceBindingHealthContributor implements CompositeHealthContributor  {
	private final SolaceFlowsHealthContributor solaceFlowsHealthContributor;
	private static final String FLOWS_CONTRIBUTOR_KEY = "flows";

	public SolaceBindingHealthContributor(SolaceFlowsHealthContributor solaceFlowsHealthContributor) {
		this.solaceFlowsHealthContributor = solaceFlowsHealthContributor;
	}

	@Override
	public HealthContributor getContributor(String name) {
		return name.equals(FLOWS_CONTRIBUTOR_KEY) ? solaceFlowsHealthContributor : null;
	}

	public SolaceFlowsHealthContributor getSolaceFlowsHealthContributor() {
		return solaceFlowsHealthContributor;
	}

	@NotNull
	@Override
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		Set<NamedContributor<HealthContributor>> contributors = Collections.singleton(
				NamedContributor.of(FLOWS_CONTRIBUTOR_KEY, solaceFlowsHealthContributor));
		return contributors.iterator();
	}
}
