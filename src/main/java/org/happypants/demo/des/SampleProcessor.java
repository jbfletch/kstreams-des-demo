package org.happypants.demo.des;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;

// Welcome and go Bills, there are some tricks to getting this to work that I will explain below
// DO not use this in production unless you really like to see things go boom, it's a demo

class SampleProcessor implements Processor<String, String> {
    private ProcessorContext context;
    private SessionStore kvStore;

    // Here is where we update the session store with the latest received value for a key
    // Note in the session window we just use the new value as the aggregate
    @Override
    public void process(String key, String value) {
        kvStore = (SessionStore) this.context.getStateStore("user-sessions");
        kvStore.put(new Windowed<>(key, new SessionWindow(0L, 6000L)),value);

    }
    @Override
    public void close() {

    }
    // The goal here is to grab all the keys in the store and emit the value every 1 min, due to the restriction
    // of retention period to stream time, we have to just the hammer known as remove to clear out old keys.
    // I always try to use a string key since there are so many examples where the ley is a long, letters are out there people!!
    // findSessions uses code point values when it looks for matching keys in the range.
    // The keyTo should be the max length of your string key, this is wonky but so is wall clock time.
    @Override
    public void init(ProcessorContext context) {

        this.context = context;

        SessionStore kvStore = (SessionStore) this.context.getStateStore("user-sessions");

        // schedule a punctuate() method every minutes based on wall_clock_time
        this.context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            final KeyValueIterator<Windowed<String>, String> iter = kvStore.findSessions("a","zzzzzzzzzzzzzz",
                    0L,Long.MAX_VALUE);

            while (iter.hasNext()) {
                KeyValue<Windowed<String>,String> entry = iter.next();
                context.forward(entry.key.toString(), entry.value.toUpperCase());
                kvStore.remove(entry.key);
            }
            iter.close();
        });
    }

}
