package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.SolaceSessionManager;
import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceBinderConfigurationProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceEndpointProvisioner;
import com.solace.spring.cloud.stream.binder.util.InitSession;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.oauth2.client.servlet.OAuth2ClientAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;


@Configuration
@Import({SolaceHealthIndicatorsConfiguration.class, OAuth2ClientAutoConfiguration.class})
@EnableConfigurationProperties({SolaceBinderConfigurationProperties.class, SolaceExtendedBindingProperties.class})
public class SolaceMessageChannelBinderConfiguration {
	private final JCSMPProperties jcsmpProperties;
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;
	private final SolaceBinderConfigurationProperties solaceBinderConfigurationProperties;
	private final SolaceSessionEventHandler solaceSessionEventHandler;

	@Nullable
	private final SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider;

	private final SolaceSessionManager solaceSessionManager;

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(JCSMPProperties jcsmpProperties,
	                                               SolaceExtendedBindingProperties solaceExtendedBindingProperties,
																								 SolaceBinderConfigurationProperties solaceBinderConfigurationProperties,
	                                               @Nullable SolaceSessionEventHandler eventHandler,
												   @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider) {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceBinderConfigurationProperties = solaceBinderConfigurationProperties;
		this.solaceSessionEventHandler = eventHandler;
		this.solaceSessionOAuth2TokenProvider = solaceSessionOAuth2TokenProvider;
		this.solaceSessionManager = new SolaceSessionManager(jcsmpProperties, solaceBinderConfigurationProperties, new SolaceBinderClientInfoProvider(),
				eventHandler, solaceSessionOAuth2TokenProvider);
	}

	/**
	 * Initialize session eagerly if failOnStartup is true (for backward compatibility).
	 */
	@PostConstruct
	public void init() {
		if (InitSession.EAGER.equals(solaceBinderConfigurationProperties.getInitSession())) {
			LOGGER.debug("Eagerly initializing Solace session due to failOnStartup=true");
			solaceSessionManager.getSession();
		} else {
			LOGGER.debug("Deferring Solace session initialization due to failOnStartup=false");
		}
	}

	/**
	 * Main binder bean - uses @Lazy to defer instantiation.
	 */
	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder(
			SolaceEndpointProvisioner solaceEndpointProvisioner,
			@Nullable ProducerMessageHandlerCustomizer<JCSMPOutboundMessageHandler> producerCustomizer,
			@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor,
			@Nullable SolaceMeterAccessor solaceMeterAccessor) {

		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(
			solaceSessionManager, solaceEndpointProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setProducerMessageHandlerCustomizer(producerCustomizer);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		binder.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
		return binder;
	}

	/**
	 * Provisioner bean - uses @Lazy to defer instantiation.
	 */
	@Bean
	SolaceEndpointProvisioner provisioningProvider() {
		return new SolaceEndpointProvisioner(solaceSessionManager);
	}

}
