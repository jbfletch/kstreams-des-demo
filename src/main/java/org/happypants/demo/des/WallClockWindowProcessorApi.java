package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Properties;

// This is a demo showing how to emit the latest received value for a given key after x amount of wall clock time
// The time interval starts then the first message for a given key is inserted in the state store
// At the end of the interval the message is forwarded and the key is deleted from the state store

public class WallClockWindowProcessorApi {

    // Create a new TimestampKeyValueStore store: FUN FACT! setting retention period does not help in the case
    // of wall clock time since it exclusively uses stream time :-)
    private final StoreBuilder<TimestampedKeyValueStore<String,String>> storeBuilder = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("key-store"),Serdes.String(),Serdes.String());


    public static void main(String[] args) {
        final WallClockWindowProcessorApi wallClockWindowProcessorApi = new WallClockWindowProcessorApi();
        KafkaStreams streams = new KafkaStreams(wallClockWindowProcessorApi.getTopology(), wallClockWindowProcessorApi.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-window-stream");
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
        builder.addSource("sourceTopic","sample-aggregation-topic")
                .addProcessor("wall-time-emit",SampleWallClockWindowProcessor::new,"sourceTopic")
                .addStateStore(storeBuilder,"wall-time-emit")
                .addSink("sinkTopic","sample-aggregate-sink-topic","wall-time-emit");
        return builder;


    }
}
