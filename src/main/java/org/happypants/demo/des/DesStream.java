package org.happypants.demo.des;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;


public class DesStream {

    public static void main(String[] args) {
        final DesStream desStream = new DesStream();
        KafkaStreams streams = new KafkaStreams(desStream.getTopology(), desStream.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "des-demo-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> baseStream = builder.stream("sample-cdc-topic");

        KStream<String, String> insertOnly = baseStream
                .filterNot((k, v) -> k.equalsIgnoreCase("7"))
                .mapValues(v -> v.toUpperCase());

        insertOnly.to("insert-topic");

        return builder.build();

    }


}
