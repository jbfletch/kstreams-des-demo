package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

// In a world where everything was amazing we would all use stream time, be personal friends with Angela Lansbury
// and drive Aston Martins; sadly this is not that world.  In light of that I present a simple example of how
// to emit the latest session value for a given key over a 1 min window. It would be madness to use this in production
// it's a demo, be safe people :-)

public class SessionWindowProcessorApi {

    // Create a new session store: FUN FACT! setting retention period does not help in the case
    // of wall clock time since it exclusively uses stream time :-)
    private final StoreBuilder<SessionStore<String,String>> storeBuilder = Stores.sessionStoreBuilder(
            Stores.persistentSessionStore("user-sessions",Duration.ofSeconds(6L)),
            Serdes.String(),
            Serdes.String()

    );


    public static void main(String[] args) {
        final SessionWindowProcessorApi sessionWindowProcessorApiStream = new SessionWindowProcessorApi();
        KafkaStreams streams = new KafkaStreams(sessionWindowProcessorApiStream.getTopology(), sessionWindowProcessorApiStream.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-sessions-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }


    Topology getTopology() {

        Topology builder = new Topology();

        //Build the topology and attach the state store to the processor
        builder.addSource("sourceTopic","sample-source-topic")
                .addProcessor("upcase",SampleProcessor::new,"sourceTopic")
                .addStateStore(storeBuilder,"upcase")
                .addSink("sinkTopic","sample-sink-topic","upcase");
        return builder;


    }
}
