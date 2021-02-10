package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import javax.xml.crypto.Data;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map;

public class JsonSerializerList<T> implements Serializer<ArrayList<T>> {
    private Serializer<T> inner;

    //Requried default constructor by Kafka
    public JsonSerializerList() {}

    public JsonSerializerList(Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
       //Filler
    }

    @Override
    public byte[] serialize(String s, ArrayList<T> ts) {
        System.out.println("Attempting to serialize arraylist");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            //objectOutputStream.writeObject(ts);
            dataOutputStream.writeInt(ts.size());
            for(int i=0;i<ts.size();i++){
                System.out.println("Serializing individual event");
                final byte[] bytes = inner.serialize(s,ts.get(i));
                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
                //objectOutputStream.writeObject(inner.serialize(s,ts.get(i)));
            }
            dataOutputStream.close();
            byteArrayOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }


    @Override
    public void close() {
       //Filler
    }
}
