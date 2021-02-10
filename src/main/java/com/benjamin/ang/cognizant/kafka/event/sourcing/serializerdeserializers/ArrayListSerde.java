package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;

public class ArrayListSerde<T> implements Serde<ArrayList<T>> {
    private final Serde<ArrayList<T>> inner;

    public ArrayListSerde(Serde<T> serde) {
        inner =
                Serdes.serdeFrom(new JsonSerializerList<>(serde.serializer())
                        ,new JsonDeserializerList<>(serde.deserializer()));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<ArrayList<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<ArrayList<T>> deserializer() {
        return inner.deserializer();
    }
}
