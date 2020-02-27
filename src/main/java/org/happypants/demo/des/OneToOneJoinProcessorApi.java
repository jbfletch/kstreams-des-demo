package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.util.Properties;

// It is a super often use case that you have an event flow that involves a non Kafka integration
// Such as Order Placed --> outside kafka integration --> look for result on topic
// This code allows you to detect the condition where the outside integration dies and returns nothing
// This is an edge case that can be detected with a session store using wall clock time
public class OneToOneJoinProcessorApi {

    // Create a new session store: FUN FACT! setting retention period does not help in the case
    // of wall clock time since it exclusively uses stream time :-)
    private final StoreBuilder<TimestampedKeyValueStore<String,String>> storeBuilder = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("pair-store"),Serdes.String(),Serdes.String());


    private final StoreBuilder<KeyValueStore<String,String>> upperBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("upper-store"),
            Serdes.String(),
            Serdes.String()

    );
    private final StoreBuilder<KeyValueStore<String,String>> baseBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("base-store"),
            Serdes.String(),
            Serdes.String()

    );


    public static void main(String[] args) {
        final OneToOneJoinProcessorApi oneToOneJoinProcessorApi = new OneToOneJoinProcessorApi();
        KafkaStreams streams = new KafkaStreams(oneToOneJoinProcessorApi.getTopology(), oneToOneJoinProcessorApi.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-one-to-one-join-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


    Topology getTopology() {

        Topology builder = new Topology();

        //Build the topology and attach the state store to the processor
        // Two topics here, one that serves as the input to the external integration
        // and one where we expect an event back
        builder.addSource("sourceTopic","sample-uppers-topic","sample-bases-topic")
                .addProcessor("pair",SampleOneToOneJoinProcessor::new,"sourceTopic")
                .addStateStore(storeBuilder,"pair")
                .addStateStore(upperBuilder,"pair")
                .addStateStore(baseBuilder,"pair")
                .addSink("sinkTopic","sample-pair-topic","pair");
        return builder;


    }
}
