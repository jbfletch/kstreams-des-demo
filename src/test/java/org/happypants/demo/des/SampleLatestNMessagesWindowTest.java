package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Properties;

class SampleLatestNMessagesWindowTest {
    private final SampleLatestNMessagesWindow app = new SampleLatestNMessagesWindow();



    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    // Verify only observations in the time window are emitted
    @Test
    void latestNValuePerKeyTest() {


        this.testTopology.input()
                .at(1000).add("1", "message one")
                .at(2000).add("1","message two")
                .at(4000).add("1","message three")
                //Emitted as a single message since the previous window has closed
                .at(12000).add("1","message next window")
                //Dropped since the window it belongs in has already closed and we have a 0 value for grace period (the default is 24 hours)
                .at(4000).add("1","message out of order and will be dropped");



        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasValue("message one")
                .expectNextRecord().hasValue("message one")
                .expectNextRecord().hasValue("message two")
                .expectNextRecord().hasValue("message two")
                .expectNextRecord().hasValue("message three")
                .expectNextRecord().hasValue("message next window")
                .expectNoMoreRecord();
    }

}
