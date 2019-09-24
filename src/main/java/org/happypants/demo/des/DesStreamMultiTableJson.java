package org.happypants.demo.des;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.happypants.demo.des.serde.ArrayListDeserializer;
import org.happypants.demo.des.serde.ArrayListSerializer;
import org.happypants.demo.des.serde.JsonSerde;

import java.util.ArrayList;
import java.util.Properties;

/**
 * A bit of background :-)
 * This example is designed to allow for easy customization, hence there may be parts that are intentionally broken out into multiple steps.
 * Everything is kept in generic JSON to allow easy manipulation of the test CDC messages in the accompanying test case.
 * This code is not production level by any stretch of the human psyche, have fun!
 *
 * Ta, @jbfletch
 *
 */

class DesStreamMultiTableJson {


    private final Serde<JsonNode> jsonSerde = new JsonSerde();

    /* Map to tell us what CDC row struct to pull based on op_type */
    private final ImmutableMap<String, String> opTypeMap = ImmutableMap.of("I", "after", "U", "after", "D", "before");

    public static void main(String[] args) {
        final DesStreamMultiTableJson desStream = new DesStreamMultiTableJson();
        KafkaStreams streams = new KafkaStreams(desStream.getTopology(), desStream.getKafkaProperties());
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "des-demo-stream-json-multi-table");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());
        return props;
    }

    Topology getTopology() {

        Serializer<ArrayList<JsonNode>> arrayListSerializer = new ArrayListSerializer<>(jsonSerde.serializer());
        Deserializer<ArrayList<JsonNode>> arrayListDeserializer = new ArrayListDeserializer<>(jsonSerde.deserializer());
        Serde<ArrayList<JsonNode>> arrayListSerde = Serdes.serdeFrom(arrayListSerializer, arrayListDeserializer);


        Predicate<String, JsonNode> isInsertOrDelete = (k, v) ->
                v.path("op_type")
                        .asText()
                        .equals("I")
                        ||
                        v.path("op_type")
                                .asText()
                                .equals("D");

        StreamsBuilder builder = new StreamsBuilder();

        /* Order Stream */
        KStream<String, JsonNode> orderStream = builder.stream("DBSCHEMA.CABOT_COVE_ORDERS", Consumed.with(Serdes.String(), jsonSerde))
                .filter(isInsertOrDelete)
                .map((k, v) -> KeyValue.pair(v.path(getOpType(v)).path("ORDER_ID").asText(), addOpType(v)));

        orderStream.to("main-order-topic");

        KTable<String, JsonNode> orderTable = builder.table("main-order-topic", Consumed.with(Serdes.String(), jsonSerde));



        /*
         Here we are able to pull the total number of items from the order stream. In some legacy systems only the total number of lines is stored.
         If you have more information, all the better, this example shows how to make it work with the bare minimum requirement.
        */
        KStream<String, JsonNode> totalNumOfOrderLines = orderStream
                .mapValues(v -> v.get("TOTAL_NUM_ITEMS"));

        totalNumOfOrderLines.to("total-items-on-order");

        KTable<String, JsonNode> totalOrderLinesTable = builder.table("total-items-on-order");


        /* Items Stream */
        KStream<String, JsonNode> orderItemsStream = builder.stream("DBSCHEMA.CABOT_COVE_ORDER_ITEMS", Consumed.with(Serdes.String(), jsonSerde))
                .filter(isInsertOrDelete)
                .map((k, v) -> KeyValue.pair(v.path(getOpType(v)).path("ORDER_ITEM_ID").asText(),addOpType(v)));

        orderItemsStream.to("order-items");


        /*
         This illustrates a method to assure you have all items for an order before you create an order aggregate
         If you have a very large number of items (subparts) the aggregate size will need to be considered
         If we receive a delete for a single item on an order, a tombstone is sent for the order key
         You can also remove the individual item and then send a tombstone when the array is empty.
         Time is a fixed resource and I am a busy be...so brevity
        */
        KTable<String, ArrayList<JsonNode>> preItemsTable = builder.stream("order-items", Consumed.with(Serdes.String(), jsonSerde))
                .groupBy((k, v) -> v.path("ORDER_ID").asText())
                .aggregate(ArrayList::new,
                        (s, jsonNode, jsonNodes) -> {
                            if(jsonNode.get("op_type").asText().equalsIgnoreCase("D")) {
                                return null;
                            } else {
                                jsonNodes.add(jsonNode);
                                return jsonNodes;
                            }

                        }
                        , Materialized.with(Serdes.String(), arrayListSerde))

                .join(totalOrderLinesTable, (jsonNodes, jsonNode) -> {
                    jsonNodes.forEach(jsonNode1 -> ((ObjectNode) jsonNode1).set("itemCount", jsonNode));
                    return jsonNodes;
                });


        /* Helpful way to see what's going on */
        preItemsTable.toStream().print(Printed.<String, ArrayList<JsonNode>>toSysOut().withLabel("preItems"));


        KTable<String, ArrayList<JsonNode>> fullItemsTable = preItemsTable
                .filterNot((k, v) -> v == null)
                .filter((k, v) -> v.size() == v.get(0).get("itemCount").asInt());


        KTable<String, JsonNode> fullOrderTable = orderTable.join(fullItemsTable, (jsonNode, jsonNodes) ->
                {
                    ObjectMapper mapper = new ObjectMapper();

                    ObjectNode rootNode = mapper.createObjectNode();
                    rootNode.set("order", jsonNode);
                    ArrayNode linesNode = mapper.createArrayNode();
                    jsonNodes.forEach(jsonNode1 -> linesNode.add(jsonNode1));
                    rootNode.set("lines", linesNode);

                    return rootNode;
                }

        );

        /* Helpful way to see what's going on */
        fullOrderTable
                .toStream()
                .filterNot((k, v) -> v == null)
                .print(Printed.<String, JsonNode>toSysOut().withLabel("FullOrder"));

        fullOrderTable
                .toStream()
                .filterNot((k, v) -> v == null)
                .to("full-order-topic", Produced.with(Serdes.String(), jsonSerde));

        return builder.build();

    }

    private String getOpType(JsonNode v) {
        return opTypeMap.get(v.path("op_type").asText());
    }

    private JsonNode addOpType(JsonNode v) {
        return ((ObjectNode) v.path(getOpType(v))).set("op_type",v.path("op_type"));
    }


}
