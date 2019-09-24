package org.happypants.demo.des.serde;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class ArrayListSerializer<T> implements Serializer<ArrayList<T>> {

    private Serializer<T> inner;

    public ArrayListSerializer(Serializer<T> inner) {
        this.inner = inner;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ArrayList<T> queue) {
        final int size = queue.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        final Iterator<T> iterator = queue.iterator();
        try {
            dos.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = inner.serialize(topic, iterator.next());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize ArrayList", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {
        inner.close();
    }
}
