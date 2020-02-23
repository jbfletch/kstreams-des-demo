package org.happypants.demo.des;

import org.apache.kafka.common.serialization.Serdes;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;


class SampleLatestNMessagesTest {
    private final SampleLatestNMessages app = new SampleLatestNMessages();

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void latestNMessagesTest(){
        this.testTopology.input()
                .add("1", "message one")
               .add("1","message two")
                .add("1","message three")
                .add("1","message four");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasKey("1").hasValue("message one")
                .expectNextRecord().hasKey("1").hasValue("message one")
               .expectNextRecord().hasKey("1").hasValue("message two")
                .expectNextRecord().hasKey("1").hasValue("message two")
                .expectNextRecord().hasKey("1").hasValue("message three")
                .expectNextRecord().hasKey("1").hasValue("message three")
                .expectNextRecord().hasKey("1").hasValue("message four")
                .expectNoMoreRecord();

    }

}
