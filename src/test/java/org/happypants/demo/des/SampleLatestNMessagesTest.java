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
                .add("1", "hello")
               .add("1","wheee")
                .add("1","NFC")
                .add("1","phil");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasKey("1").hasValue("hello")
                .expectNextRecord().hasKey("1").hasValue("hello")
               .expectNextRecord().hasKey("1").hasValue("wheee")
                .expectNextRecord().hasKey("1").hasValue("wheee")
                .expectNextRecord().hasKey("1").hasValue("NFC")
                .expectNextRecord().hasKey("1").hasValue("NFC")
                .expectNextRecord().hasKey("1").hasValue("phil")
                .expectNoMoreRecord();

    }

}
