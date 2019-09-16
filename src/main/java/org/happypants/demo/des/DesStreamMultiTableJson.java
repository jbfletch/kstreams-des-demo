package org.happypants.demo.des;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.happypants.demo.des.serde.JsonSerde;

import java.util.Properties;


public class DesStreamMultiTableJson {


    private final Serde<JsonNode> jsonSerde = new JsonSerde();


    public static void main(String[] args) {
        final DesStreamMultiTableJson desStream = new DesStreamMultiTableJson();
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

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> orderStream = builder.stream("SCHEMA.ORDER", Consumed.with(Serdes.String(),jsonSerde))
                .filter(isInsert)
                .map((k,v) -> KeyValue.pair(v.path("after").path("ORDERNUMBER").asText(),v.get("after")));

        KStream<String,JsonNode> totalNumOfOrderLines = orderStream
                .mapValues(v -> v.path("TOTALNUMLINES"));

        KStream<String, JsonNode> orderLinesStream = builder.stream("SCHEMA.ORDERLINES", Consumed.with(Serdes.String(),jsonSerde))
                .filter(isInsert)
                .map((k,v) -> KeyValue.pair(v.path("after").path("ORDERNUMBER").asText(),v.get("after")));


        KTable<String, JsonNode> 



        KTable<String,JsonNode> orderLineNumbers = orderStream
                .map

        KStream<String, JsonNode> insertOnly = baseStream
                .filter(isInsert)
                .mapValues(v -> v.path("before"));

        insertOnly.to("insert-topic");


        return builder.build();

    }


}
