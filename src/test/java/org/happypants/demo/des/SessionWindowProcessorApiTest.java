package org.happypants.demo.des;

import org.apache.kafka.common.serialization.Serdes;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SessionWindowProcessorApiTest {
    private final SessionWindowProcessorApi app = new SessionWindowProcessorApi();

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

// This mimics multiple updates to the same key and tests that we only get the last update
// We need to advance wall clock time to trigger the punctuate and emit the records to the sink topic
    @Test
    void uppercaseFilterTest() {
        this.testTopology.input()
                .add("a", "first")
                .add("a", "second")
                .add("a", "third")
                .add("bbb","angela lansbury");

        this.testTopology.getTestDriver().advanceWallClockTime(60000);


        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasValue("THIRD")
                .expectNextRecord().hasValue("ANGELA LANSBURY")
                .expectNoMoreRecord();

    }

}
