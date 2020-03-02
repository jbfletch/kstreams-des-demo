package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.happypants.demo.des.serde.LinkedListDeserializer;
import org.happypants.demo.des.serde.LinkedListSerializer;

import java.sql.Time;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Materialized.as;

/**
 * A bit of background :-)
 * This example is designed to allow for easy customization, hence there may be parts that are intentionally broken out into multiple steps.
 * This demonstrates keeping and emitting the last n values (in our example n == 2) for a given key, inside a set window.
 * Can be a useful way to deal with the desire for an overnight/hour/n reset pattern
 * This code is not production level by any stretch of the human psyche, have fun!
 *
 * Ta, @jbfletch
 *
 */


class SampleLatestNMessagesWindow {

    public static void main(String[] args) {
        final SampleLatestNMessagesWindow sampleLatestNMessagesWindow = new SampleLatestNMessagesWindow();
        KafkaStreams streams = new KafkaStreams(sampleLatestNMessagesWindow.getTopology(), sampleLatestNMessagesWindow.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "window-latest-n-messages");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        Serializer<LinkedList<String>> linkedListSerializer = new LinkedListSerializer<>(new StringSerializer());
        Deserializer<LinkedList<String>> linkedListDeserializer = new LinkedListDeserializer<>(new StringDeserializer());
        Serde<LinkedList<String>> linkedListSerde = Serdes.serdeFrom(linkedListSerializer, linkedListDeserializer);


        // Create a KTable that holds the last to values for a given key as an aggregate, linked lists work nicely since they are fifo
        // We remove the oldest value for a given key when we have 2 in the list
        // This example sets a window of 10 seconds with 0 grace period, this allows strict window boundaries and as such late arriving records are dropped
        KTable<Windowed<String>, LinkedList<String>> preItemsTable = builder.stream("sample-num-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10L)).grace(Duration.ofSeconds(0)))
                .aggregate(LinkedList::new,
                        (s, newMessage,  messageList) -> {
                            if(messageList.size()==2) {
                              messageList.remove();
                              messageList.add(newMessage);
                            } else {
                                messageList.add(newMessage);
                            }
                            return messageList;
                        }
                        ,Materialized.with(Serdes.String(),linkedListSerde));



        // Flat Map the linkedlist to individual messages
        KStream<Windowed<String>, String> result = preItemsTable.toStream().flatMapValues(v -> v);

        // Helpful to see the flow
        result.print(Printed.<Windowed<String>, String>toSysOut().withLabel("nope"));

        result.to("test topic");

        return builder.build();

    }


}
