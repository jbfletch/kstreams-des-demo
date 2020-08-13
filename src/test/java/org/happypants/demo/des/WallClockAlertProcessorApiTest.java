package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class WallClockAlertProcessorApiTest {
    private final WallClockAlertProcessorApi app = new WallClockAlertProcessorApi();
    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-window-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TardisTimestampExtractor.class);
        return props;
    }

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, getKafkaProperties());


    //TODO: 12/24/19 NOT WORKING HAVE TO FIX, GETTING CALLED A GRINCH WILL FIX DAY AFTER CHRISTMAS

// This demonstrates how the message with a key of a is excluded from the output topic because it successfully came
// back from the external integration, orders with keys of b and ccc are sent to the alert topic as they have not come back
// NOTE: d is not included since it is a new session and is not subject to the range we set in the punctuate

    @Test
    void uppercaseFilterTest() throws InterruptedException {


        this.testTopology.input("sample-order-placed-topic")
                .add("a", "order 1 Placed")
                .add("b", "order 2 placed")
                .add("c","order 3 placed");

        this.testTopology.input("sample-invoice-created-topic")
            .add("a","invoice created");


        this.testTopology.getTestDriver().advanceWallClockTime(20000L);


        this.testTopology.tableOutput("sample-sink-topic").withSerde(Serdes.String(), Serdes.String())


            .expectNextRecord().hasKey("b")
            .expectNextRecord().hasKey("c")
            .expectNoMoreRecord();


    }

}
