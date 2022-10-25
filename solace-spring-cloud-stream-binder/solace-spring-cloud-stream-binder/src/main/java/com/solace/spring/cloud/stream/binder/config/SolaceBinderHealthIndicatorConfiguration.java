package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.properties.SolaceHealthSessionProperties;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionHealthIndicator;
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
@EnableConfigurationProperties(SolaceHealthSessionProperties.class)
public class SolaceBinderHealthIndicatorConfiguration {

    private static final Log logger = LogFactory.getLog(SolaceBinderHealthIndicatorConfiguration.class);

    @Bean
    public SolaceSessionHealthIndicator solaceBinderHealthIndicator(SolaceHealthSessionProperties solaceHealthProperties) {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating Solace Binder Health Indicator");
        }
        return new SolaceSessionHealthIndicator(solaceHealthProperties);
    }

    @Bean
    public SolaceSessionEventHandler solaceSessionEventHandler(SolaceSessionHealthIndicator solaceSessionHealthIndicator) {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating Solace Session Event Handler");
        }
        return new SolaceSessionEventHandler(solaceSessionHealthIndicator);
    }

}
