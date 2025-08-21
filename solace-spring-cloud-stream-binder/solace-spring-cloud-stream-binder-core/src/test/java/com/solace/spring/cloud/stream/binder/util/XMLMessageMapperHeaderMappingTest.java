package com.solace.spring.cloud.stream.binder.util;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class XMLMessageMapperHeaderMappingTest {

  @Spy
  private XMLMessageMapper xmlMessageMapper;
}