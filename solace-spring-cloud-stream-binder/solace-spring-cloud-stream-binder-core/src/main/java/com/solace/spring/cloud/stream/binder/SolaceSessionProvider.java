package com.solace.spring.cloud.stream.binder;

import com.solacesystems.jcsmp.JCSMPSession;

public interface SolaceSessionProvider {

  JCSMPSession getSession();
}