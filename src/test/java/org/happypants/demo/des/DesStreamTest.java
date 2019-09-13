package org.happypants.demo.des;

import org.apache.kafka.common.serialization.Serdes;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;


class DesStreamTest {
    private final DesStream app = new DesStream();

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void uppercaseFilterTest(){
        this.testTopology.input()
                .add("1", "hello")
                .add("2","angela lansbury")
                .add("7","not on my filters watch");

        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.String())
                .expectNextRecord().hasKey("1").hasValue("HELLO")
                .expectNextRecord().hasKey("2").hasValue("ANGELA LANSBURY")
                .expectNoMoreRecord();

    }

}
