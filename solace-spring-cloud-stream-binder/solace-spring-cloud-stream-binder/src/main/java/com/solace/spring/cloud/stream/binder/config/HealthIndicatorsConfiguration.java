package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.health.contributors.BindingsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.ConnectionHealthContributor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceFlowHealthProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
@EnableConfigurationProperties({SolaceSessionHealthProperties.class, SolaceFlowHealthProperties.class})
public class HealthIndicatorsConfiguration {
	private final SolaceSessionHealthProperties solaceSessionHealthProperties;
	private final SolaceFlowHealthProperties solaceFlowHealthProperties;
	private static final Log logger = LogFactory.getLog(HealthIndicatorsConfiguration.class);

	public HealthIndicatorsConfiguration(SolaceSessionHealthProperties solaceSessionHealthProperties,
	                                     SolaceFlowHealthProperties solaceFlowHealthProperties) {
		this.solaceSessionHealthProperties = solaceSessionHealthProperties;
		this.solaceFlowHealthProperties = solaceFlowHealthProperties;
	}

	@Bean
	public ConnectionHealthContributor solaceSessionHealthContributor() {
		if (logger.isDebugEnabled()) {
			logger.debug("Creating Solace Connection Health Indicators Hierarchy");
		}
		return new ConnectionHealthContributor(
				new SessionHealthIndicator(solaceSessionHealthProperties),
				new BindingsHealthContributor(solaceFlowHealthProperties)
		);
	}

	@Bean
	public SolaceSessionEventHandler solaceSessionEventHandler(ConnectionHealthContributor healthContributor) {
		if (logger.isDebugEnabled()) {
			logger.debug("Creating Solace Session Event Handler for monitoring Health");
		}
		return new SolaceSessionEventHandler(healthContributor.getSolaceSessionHealthIndicator());
	}

}
