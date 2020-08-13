package org.happypants.demo.des;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;


// Welcome and go Bills, there are some tricks to getting this to work that I will explain below
// DO not use this in production unless you really like to see things go boom, it's a demo

class SampleWallClockAlertProcessor implements Processor<String, String> {
    private ProcessorContext context;


    // This is the sneaky sauce, we use a cancel approach to call out events we never get a result for
    // If we get a result from the external integration we remove it from the state store
    // This will mean that only one sided (event sent but no result received) will remain in the store
    @Override
    public void process(String key, String value) {
        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("alert-store");

        Optional<ValueAndTimestamp> tsValue = Optional.ofNullable((ValueAndTimestamp) kvStore.get(key));
        if (tsValue.isPresent()) {
            kvStore.delete(key);
        } else {
            System.out.println("this is the context timestamp: "+ context.timestamp());
            kvStore.put(key, ValueAndTimestamp.make(value, context.timestamp()));
        }

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


        TimestampedKeyValueStore kvStore = (TimestampedKeyValueStore) this.context.getStateStore("alert-store");

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
