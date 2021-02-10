package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;


public class JsonDeserializerList<T> implements Deserializer<ArrayList<T>> {
    private Deserializer<T> valueDeserializer;

    //Requried default constructor by Kafka
    public JsonDeserializerList() { }

    public JsonDeserializerList(Deserializer<T> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Filler
    }

    @Override
    public ArrayList<T> deserialize(String s, byte[] bytes) {
        System.out.println("Deserializing arraylist !");
        if (bytes == null || bytes.length == 0)  {
            return null;
        }
        ArrayList<T> arrayList = new ArrayList<>();
        try {
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            final int noRecords = dataInputStream.readInt();
            for(int i=0;i<noRecords;i++) {
                final byte[] size = new byte[dataInputStream.readInt()];
                dataInputStream.read(size);
                arrayList.add(valueDeserializer.deserialize(s,size));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    @Override
    public void close() {
        //Filler
    }
}
