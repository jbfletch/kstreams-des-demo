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

class DesStreamJsonTest {

    private final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    private final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    private final DesStreamJson app = new DesStreamJson();
    private ObjectMapper mapper = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    @RegisterExtension
    final TestTopologyExtension<Object, JsonNode> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void uppercaseFilterTest()
            throws IOException {


        JsonNode node1 = mapper.readValue("{'orderNumber' : '123'}", JsonNode.class);
        JsonNode node2 = mapper.readValue("{'table':'DBSCHEMA.CABOT_COVE_CUSTOMERS', 'op_type':'I', 'op_ts':'2019-10-16 17:34:20.000534', 'current_ts':'2019-09-16 17:34:20.000534', 'pos':'00000000000000000000000042', 'before':{ 'ORDERS_STATUS_HISTORY_ID':'8675309', 'ORDERS_ID':7, 'ORDERS_STATUS':0, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'CUSTOMER_NOTIFIED':1, 'COMMENTS':'Order received, customer notified' }, 'after':{ 'ORDERS_STATUS_HISTORY_ID':'1016', 'ORDERS_ID':7, 'ORDERS_STATUS':1, 'DATE_ADDED':'2016-06-16:13:32:23.172212000', 'CUSTOMER_NOTIFIED':1, 'COMMENTS':'Order received, customer notified'}}", JsonNode.class);


        this.testTopology.input()
                .add("1", node1)
                .add("2", node2);

        this.testTopology.tableOutput().withSerde(Serdes.String(), jsonSerde)
                .expectNextRecord().hasKey("2").hasValue(node2.path("before"));

    }

}
