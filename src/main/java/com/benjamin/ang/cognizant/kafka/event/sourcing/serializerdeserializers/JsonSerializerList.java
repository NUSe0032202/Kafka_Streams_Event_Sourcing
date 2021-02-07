package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;

public class JsonSerializerList<T> implements Serializer<ArrayList<T>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, ArrayList<T> ts) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
