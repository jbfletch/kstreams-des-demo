package org.happypants.demo.des.serde;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LinkedListSerializer<T> implements Serializer<LinkedList<T>> {

    private Serializer<T> inner;

    public LinkedListSerializer(Serializer<T> inner) {
        this.inner = inner;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, LinkedList<T> linkedList) {
        final int size = linkedList.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        final Iterator<T> iterator = linkedList.iterator();
        try {
            dos.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = inner.serialize(topic, iterator.next());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize Queue", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {
        inner.close();
    }
}
