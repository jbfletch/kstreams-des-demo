package org.happypants.demo.des;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;


// Welcome and go Bills, there are some tricks to getting this to work that I will explain below
// DO not use this in production unless you really like to see things go boom, it's a demo

class SampleWallClockWindowProcessor implements Processor<String, String> {
    private ProcessorContext context;


// Easy Peasy: We check to see if the key is already in the store and create a new record with the updated value
// but retain the original timestamp, if there is no entry in the key value store we insert it

    @Override
    public void process(String key, String value) {
        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("key-store");

         Optional<ValueAndTimestamp> tsValue = Optional.ofNullable((ValueAndTimestamp) kvStore.get(key));
        if (tsValue.isPresent()) {
                  ValueAndTimestamp<String> newRecord = ValueAndTimestamp.make(value,tsValue.get().timestamp());
                  kvStore.put(key,newRecord);
        } else {
            System.out.println("this is the context timestamp: "+ context.timestamp());
            kvStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
        }



    }

    @Override
    public void close() {

    }

    // Each punctuate we iterate through all of the keys and emit any keys that
    // were first added to the the store over a 60 seconds ago
    @Override
    public void init(ProcessorContext context) {

        this.context = context;

        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("key-store");

        // schedule a punctuate() method every 20 seconds, this will iterate through ALL of the keys every time
        // test performance if you have a large number, it may also skip punctuating if you set the duration too low
        // and there are a large number of keys to iterate though
        this.context.schedule(Duration.ofSeconds(20L), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            final KeyValueIterator<String, ValueAndTimestamp> iter = kvStore.all();

            while (iter.hasNext()) {
                KeyValue<String, ValueAndTimestamp> entry = iter.next();
                Optional<KeyValue<String, ValueAndTimestamp>> message = Optional.of(entry);

                // This allows the time to compare to continually update as you iterate thought.
                // The other option is to set a fixed time and then pass that into the filter
                message.filter(e -> Instant.ofEpochMilli(e.value.timestamp()).isBefore(Instant.now().minusSeconds(60)))
                        .ifPresent(e -> {
                            this.context.forward(e.key,e.value.value());
                            kvStore.delete(e.key);
                        });

            }
            iter.close();
        });
    }

}
