package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Map;

public class JsonDeserializerList<T> implements Deserializer<ArrayList<T>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ArrayList<T> deserialize(String s, byte[] bytes) {
        return null;
    }


    @Override
    public void close() {

    }
}
