package com.solace.spring.cloud.stream.binder.properties;


import com.solace.spring.cloud.stream.binder.util.InitSession;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.cloud.stream.solace.binder")
public class SolaceBinderConfigurationProperties {


  private InitSession initSession = InitSession.EAGER;

  public InitSession getInitSession() {
    return initSession;
  }

  public void setInitSession(InitSession initSession) {
    this.initSession = initSession;
  }
}