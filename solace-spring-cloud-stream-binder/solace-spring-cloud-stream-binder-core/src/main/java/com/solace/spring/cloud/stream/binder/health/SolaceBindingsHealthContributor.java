package com.solace.spring.cloud.stream.binder.health;

import org.jetbrains.annotations.NotNull;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SolaceBindingsHealthContributor implements CompositeHealthContributor {
	private final Map<String, SolaceBindingHealthContributor> contributors = new HashMap<>();

	public void addBindingContributor(String bindingName, SolaceBindingHealthContributor bindingHealthContributor) {
		contributors.put(bindingName, bindingHealthContributor);
	}

	public void removeBindingContributor(String bindingName) {
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
