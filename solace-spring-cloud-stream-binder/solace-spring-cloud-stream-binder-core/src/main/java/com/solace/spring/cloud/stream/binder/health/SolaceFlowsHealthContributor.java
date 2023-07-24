package com.solace.spring.cloud.stream.binder.health;

import org.jetbrains.annotations.NotNull;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SolaceFlowsHealthContributor implements CompositeHealthContributor {
	private final Map<String, SolaceFlowHealthIndicator> contributors = new HashMap<>();

	public void addFlowContributor(String bindingName, SolaceFlowHealthIndicator flowHealthContributor) {
		contributors.put(bindingName, flowHealthContributor);
	}

	public void removeFlowContributor(String bindingName) {
		contributors.remove(bindingName);
	}

	@Override
	public HealthContributor getContributor(String name) {
		return contributors.get(name);
	}

	@NotNull
	@Override
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		return contributors.entrySet()
				.stream()
				.map((entry) -> NamedContributor.of(entry.getKey(), (HealthContributor) entry.getValue()))
				.iterator();
	}
}
