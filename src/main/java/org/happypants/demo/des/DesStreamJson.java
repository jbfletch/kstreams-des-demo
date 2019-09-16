package org.happypants.demo.des;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.happypants.demo.des.serde.JsonSerde;

import java.util.Properties;


public class DesStreamJson {


    private final Serde<JsonNode> jsonSerde = new JsonSerde();


    public static void main(String[] args) {
        final DesStreamJson desStream = new DesStreamJson();
        KafkaStreams streams = new KafkaStreams(desStream.getTopology(), desStream.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "des-demo-stream-json");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());
        return props;
    }

    Topology getTopology() {

        Predicate<String, JsonNode> isInsert = (k, v) ->
                v.path("op_type")
                        .asText()
                        .equals("I");

        Predicate<String, JsonNode> isNewOrder = (k, v) ->
                v.path("before").path("ORDERS_ID")
                        .asText()
                        .equals("null")
                        &&
                        !v.path("after").path("ORDERS_ID")
                                .asText()
                                .equals("null");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> baseStream = builder.stream("DBSCHEMA.CABOT_COVE_ORDERS");

        KStream<String, JsonNode> insertOnly = baseStream
                .filter(isInsert)
                .filter(isNewOrder)
                .mapValues(v -> v.path("before"));

        insertOnly.to("event-order-created");


        return builder.build();

    }


}
