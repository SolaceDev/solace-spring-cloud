package com.solace.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.provisioning.ProducerDestination;

class SolaceProducerDestination implements ProducerDestination {
    private final String destinationName;

    SolaceProducerDestination(String destinationName) {
        this.destinationName = destinationName;
    }

    @Override
    public String getName() {
        return destinationName;
    }

    @Override
    public String getNameForPartition(int partition) {
        return destinationName;
    }

    @Override
    public String toString() {
        String sb = "SolaceProducerDestination{" + "destinationName='" + destinationName + '\'' +
                '}';
        return sb;
    }
}
