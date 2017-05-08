package io.advantageous.reakt.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AsyncProducerTest {

    @Test
    public void sendRecord() throws Exception {

        final AsyncProducer<String, String> producer = new AsyncProducer<>(
                new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        final RecordMetadata recordMetadata = producer.sendRecord(new ProducerRecord<String, String>("topic1", "value1"))
                .blockingGet(Duration.ofSeconds(5));

        assertNotNull(recordMetadata);
    }

    @Test
    public void send() throws Exception {

        final AsyncProducer<String, String> producer = new AsyncProducer<>(
                new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        final RecordMetadata recordMetadata = producer.send("topic1", "key1", "value1")
                .blockingGet(Duration.ofSeconds(5));

        assertNotNull(recordMetadata);

        assertNotNull(producer.getProducer());
        producer.flush();
        producer.close();
    }

    @Test (expected = CommitFailedException.class)
    public void sendFail() throws Exception {



        final AsyncProducer<String, String> producer = new AsyncProducer<>(new Producer<String, String>() {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
                return null;
            }

            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {

                callback.onCompletion(null, new CommitFailedException());
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends Metric> metrics() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long timeout, TimeUnit unit) {

            }
        });


        final RecordMetadata recordMetadata = producer.send("topic1", "key1", "value1")
                .blockingGet(Duration.ofSeconds(5));


        assertNotNull(recordMetadata);
    }
}