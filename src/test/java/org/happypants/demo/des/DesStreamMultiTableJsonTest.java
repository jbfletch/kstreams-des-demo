package org.happypants.demo.des;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;


class DesStreamMultiTableJsonTest {



    private final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    private final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    private final DesStreamMultiTableJson app = new DesStreamMultiTableJson();
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    @RegisterExtension
    final TestTopologyExtension<Object, JsonNode> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void cdcItemBlockerTest()
            throws IOException {

        JsonNode node2 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_ORDERS', 'op_type':'I', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'after':{ 'ORDERS_STATUS_HISTORY_ID':'42', 'ORDER_ID':7, 'ORDERS_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'CUSTOMER_NOTIFIED':1, 'TOTAL_NUM_ITEMS':3, 'COMMENTS':'Order received, customer notified'}}", JsonNode.class);
        JsonNode node3 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_ORDER_ITEMS', 'op_type':'I', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'after':{ 'ORDERS_LINE_STATUS_HISTORY_ID':'42', 'ORDER_ITEM_ID':18, 'ORDER_ID':7, 'ORDERS_LINE_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'ITEM_COMMENTS':'Scooters are the best'}}", JsonNode.class);
        JsonNode node4 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_ORDER_ITEMS', 'op_type':'I', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'after':{ 'ORDERS_LINE_STATUS_HISTORY_ID':'42', 'ORDER_ITEM_ID':19, 'ORDER_ID':7, 'ORDERS_LINE_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'ITEM_COMMENTS':'High-Tops are the best'}}", JsonNode.class);
        JsonNode node5 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_ORDER_ITEMS', 'op_type':'I', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'after':{ 'ORDERS_LINE_STATUS_HISTORY_ID':'42', 'ORDER_ITEM_ID':20, 'ORDER_ID':7, 'ORDERS_LINE_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'ITEM_COMMENTS':'DJ Cat Scratch rules'}}", JsonNode.class);
        JsonNode node6 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_ORDER_ITEMS', 'op_type':'D', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'before':{ 'ORDERS_LINE_STATUS_HISTORY_ID':'42', 'ORDER_ITEM_ID':20, 'ORDER_ID':7, 'ORDERS_LINE_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'ITEM_COMMENTS':'DJ Cat Scratch rules'}}", JsonNode.class);


        /* This is a fun place to switch up the order of sending and see how that impacts things :-)  */
        this.testTopology.input("DBSCHEMA.CABOT_COVE_ORDERS")
                .add(null, node2);

        this.testTopology.input("DBSCHEMA.CABOT_COVE_ORDER_ITEMS")
                .add(null,node3)
                .add(null,node4)
                .add(null,node5);

        this.testTopology.input("DBSCHEMA.CABOT_COVE_ORDER_ITEMS")
                .add(null,node6); // Delete to demonstrate tombstoning


        this.testTopology.tableOutput("full-order-topic").withSerde(Serdes.String(),jsonSerde).expectNextRecord().isPresent();


    }
}
