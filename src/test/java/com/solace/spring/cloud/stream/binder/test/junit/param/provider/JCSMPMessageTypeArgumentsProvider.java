package com.solace.spring.cloud.stream.binder.test.junit.param.provider;

import com.solacesystems.jcsmp.*;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junitpioneer.jupiter.cartesian.CartesianParameterArgumentsProvider;

import java.lang.reflect.Parameter;
import java.util.stream.Stream;

public class JCSMPMessageTypeArgumentsProvider implements ArgumentsProvider,
        CartesianParameterArgumentsProvider<Class<? extends BytesXMLMessage>> {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return provideArguments(context, null)
                .map(Arguments::of);
    }

    @Override
    public Stream<Class<? extends BytesXMLMessage>> provideArguments(ExtensionContext context, Parameter parameter) {
        return Stream.of(TextMessage.class,
                BytesMessage.class,
                XMLContentMessage.class,
                MapMessage.class,
                StreamMessage.class);
    }
}
