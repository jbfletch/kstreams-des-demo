package org.happypants.demo.des;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

// Custom Timestamp extractor that sets the timestamp for records to be in the past, it's the doctor.
public class TardisTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return System.currentTimeMillis() - (60000) ;
    }
}
