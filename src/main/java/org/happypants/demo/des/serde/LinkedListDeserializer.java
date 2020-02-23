package org.happypants.demo.des.serde;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LinkedListDeserializer<T> implements Deserializer<LinkedList<T>> {
    private final Deserializer<T> valueDeserializer;

    public LinkedListDeserializer(final Deserializer<T> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public LinkedList<T> deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        final LinkedList<T> linkedList = new LinkedList<T>() {};
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                dataInputStream.read(valueBytes);
                linkedList.add(valueDeserializer.deserialize(topic, valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize Queue", e);
        }

        return linkedList;
    }

    @Override
    public void close() {
        // do nothing
    }
}
