package com.solace.spring.cloud.stream.binder.properties;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SmfMessageReaderProperties {

  private Set<String> headerExclusions;
  private Map<String, String> headerToUserPropertyKeyMapping;

  public SmfMessageReaderProperties(SolaceConsumerProperties solaceConsumerProperties) {
    this.headerExclusions = new HashSet<>(Objects.requireNonNullElse(solaceConsumerProperties.getHeaderExclusions(), Set.of()));
    this.headerToUserPropertyKeyMapping = new LinkedHashMap<>(Objects.requireNonNullElse(solaceConsumerProperties.getHeaderToUserPropertyKeyMapping(), Map.of()));
  }

  public Set<String> getHeaderExclusions() {
    return headerExclusions;
  }

  public Map<String, String> getHeaderToUserPropertyKeyMapping() {
    return headerToUserPropertyKeyMapping;
  }
}