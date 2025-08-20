package com.solace.spring.cloud.stream.binder.properties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SmfMessageReaderProperties {

  private Set<String> headerExclusions;
  private Map<String, String> headerToUserPropertyKeyMapping;

  public SmfMessageReaderProperties(SolaceConsumerProperties solaceConsumerProperties) {
    this.headerExclusions = new HashSet<>(solaceConsumerProperties.getHeaderExclusions());
    this.headerToUserPropertyKeyMapping = new HashMap<>(solaceConsumerProperties.getHeaderToUserPropertyKeyMapping());
  }

  public Set<String> getHeaderExclusions() {
    return headerExclusions;
  }

  public Map<String, String> getHeaderToUserPropertyKeyMapping() {
    return headerToUserPropertyKeyMapping;
  }
}