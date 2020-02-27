package org.happypants.demo.des;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;


// Welcome and go Bills, there are some tricks to getting this to work that I will explain below
// DO not use this in production unless you really like to see things go boom, it's a demo

class SampleOneToOneJoinProcessor implements Processor<String, String> {
    private ProcessorContext context;


    // This is the sneaky sauce, we use a cancel approach to call out events we never get a result for
    // If we get a result from the external integration we remove it from the state store
    // This will mean that only one sided (event sent but no result received) will remain in the store
    @Override
    public void process(String key, String value) {
        KeyValueStore upperStore = (KeyValueStore) this.context.getStateStore("upper-store");
        KeyValueStore baseStore = (KeyValueStore) this.context.getStateStore("base-store");
        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("pair-store");

        if(context.topic().equalsIgnoreCase("sample-uppers-topic")) {
            upperStore.put(key,value);
        } else if(context.topic().equalsIgnoreCase("sample-bases-topic")){
            baseStore.put(key,value);
        }

//        Optional<KeyValueIterator<Windowed<String>, String>> there = Optional.ofNullable(kvStore.get());
//        if (there.get().hasNext()) {
//            kvStore.remove(there.get().next().key);
//            there.get().close();
//        } else {
//            kvStore.put(new Windowed<>(key, new SessionWindow(context.timestamp(), context.timestamp() + 60000L)), value);
//        }


        //
    }

    @Override
    public void close() {

    }

    // This is a case where the range we look for matters, we only want keys that are still there
    // after whatever our time frame for detecting no response is, in the example below
    // We want to know if we send an event to an external integration and never hear back after a minute
    // Sessions that have not been sitting around for a minute are ignored due to the range criteria we set
    // for latest session start time
    @Override
    public void init(ProcessorContext context) {

        this.context = context;

        KeyValueStore upperStore = (KeyValueStore) this.context.getStateStore("upper-store");
        KeyValueStore baseStore = (KeyValueStore) this.context.getStateStore("base-store");
        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("pair-store");


        // schedule a punctuate() method every minutes based on wall_clock_time
        this.context.schedule(Duration.ofSeconds(20L), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            final KeyValueIterator<String,String> iter = baseStore.all();

            while (iter.hasNext()) {
                KeyValue<String,String> entry = iter.next();
                Optional<KeyValue<String,String>> anna = Optional.of(entry);

                this.context.forward(anna.get().key, anna.get().value);

//                anna.filter(e -> e.key.window().endTime().isBefore(Instant.now()))
//                        .ifPresent(e -> {
//                            this.context.forward(e.key.toString(), e.value.toUpperCase());
//                            kvStore.remove(e.key);
//                        });

            }
            iter.close();
        });
    }

}
