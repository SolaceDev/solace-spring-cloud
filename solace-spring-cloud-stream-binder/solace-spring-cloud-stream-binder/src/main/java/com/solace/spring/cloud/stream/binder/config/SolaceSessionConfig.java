package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.util.DefaultSolaceSessionManager;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionManager;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

@Configuration
public class SolaceSessionConfig {

  @Bean
  public SolaceSessionManager solaceSessionManager(JCSMPProperties jcsmpProperties,
      @Nullable SolaceSessionEventHandler eventHandler,
      @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider) {
    return new DefaultSolaceSessionManager(jcsmpProperties, new SolaceBinderClientInfoProvider(),
        eventHandler, solaceSessionOAuth2TokenProvider);
  }
}