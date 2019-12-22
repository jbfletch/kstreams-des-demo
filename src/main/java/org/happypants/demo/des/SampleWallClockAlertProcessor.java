package org.happypants.demo.des;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
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
        SessionStore kvStore = (SessionStore) this.context.getStateStore("alert-store");
        Optional<KeyValueIterator<Windowed<String>, String>> there = Optional.ofNullable(kvStore.fetch(key));
        if(there.get().hasNext()){
            kvStore.remove(there.get().next().key);
        }else {
            kvStore.put(new Windowed<>(key, new SessionWindow(0L, 50000L)),value);
        }

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

        SessionStore kvStore = (SessionStore) this.context.getStateStore("alert-store");

        // schedule a punctuate() method every minutes based on wall_clock_time
        this.context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            final KeyValueIterator<Windowed<String>, String> iter = kvStore.findSessions("a","zzzzzzzzzzzzzz",
                    0L,System.currentTimeMillis()- 10000L);

            while (iter.hasNext()) {
                KeyValue<Windowed<String>,String> entry = iter.next();
                context.forward(entry.key.toString(), entry.value.toUpperCase());
                kvStore.remove(entry.key);
            }
            iter.close();
        });
    }

}
