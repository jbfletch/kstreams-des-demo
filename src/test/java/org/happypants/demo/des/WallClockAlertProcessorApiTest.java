package org.happypants.demo.des;

import org.apache.kafka.common.serialization.Serdes;
import org.happypants.kafka.testHelpers.TestTopologyExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

class WallClockAlertProcessorApiTest {
    private final WallClockAlertProcessorApi app = new WallClockAlertProcessorApi();

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology =
            new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());


    //TODO: 12/24/19 NOT WORKING HAVE TO FIX, GETTING CALLED A GRINCH WILL FIX DAY AFTER CHRISTMAS

// This demonstrates how the message with a key of a is excluded from the output topic because it successfully came
// back from the external integration, orders with keys of b and ccc are sent to the alert topic as they have not come back
// NOTE: d is not included since it is a new session and is not subject to the range we set in the punctuate

    @Test
    void uppercaseFilterTest() throws InterruptedException {


        this.testTopology.input("sample-order-placed-topic2")
                .add("a", "order 1 Placed")
                .add("b", "order 2 placed")
                .add("ccc","order 3 placed");

        this.testTopology.getTestDriver().advanceWallClockTime(60000L);

        //this.testTopology.getTestDriver().advanceWallClockTime(20);

//        this.testTopology.input("sample-invoice-created-topic2")
//                .add("a","invoice created");

       //this.testTopology.getTestDriver().advanceWallClockTime(10000L);

        //this.testTopology.input("sample-order-placed-topic2")
                //.add("d", "order 4 Placed");



        this.testTopology.tableOutput("sample-sink-topic").withSerde(Serdes.String(), Serdes.String())
//
//
//              .expectNextRecord().hasValue("ORDER 2 PLACED")
//             .expectNextRecord().hasValue("ORDER 3 PLACED")
                .expectNoMoreRecord();


     //   this.testTopology.getTestDriver().advanceWallClockTime(30000);

//        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.String())
//                .expectNextRecord().hasValue("ORDER 2 PLACED")
//                .expectNextRecord().hasValue("ORDER 3 PLACED")
//                .expectNoMoreRecord();

    }

}
