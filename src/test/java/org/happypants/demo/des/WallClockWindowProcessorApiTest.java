package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Properties;

class WallClockWindowProcessorApiTest {
    private final WallClockWindowProcessorApi app = new WallClockWindowProcessorApi();

    // Setting properties in the test directly as we need to use a custom TimestampExtractor
    // and I dislike (<-- understatement) additional folders or files in demo code
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

    // Verify the latest value for a is emitted
    @Test
    void latestValuePerKeyTest() {


        this.testTopology.input()
                .add("a", "order 1 Placed")
                .add("a", "order 2 placed")
                .add("ccc", "order 3 placed");

        this.testTopology.getTestDriver().advanceWallClockTime(60000L);


        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasValue("order 2 placed")
                .expectNextRecord().hasValue("order 3 placed")
                .expectNoMoreRecord();
    }

}
