package io.advantageous.reakt.kafka;

import io.advantageous.reakt.Stream;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;

public class StreamConsumerTest {
    @Test
    public void subscribe() throws Exception {

        final MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        mockConsumer.assign(asList(new TopicPartition("my_topic", 0)));

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);


        mockConsumer.addRecord(new ConsumerRecord<>("my_topic",
                0, 0L, "mykey", "myvalue0"));
        mockConsumer.addRecord(new ConsumerRecord<>("my_topic", 0,
                1L, "mykey", "myvalue1"));
        mockConsumer.addRecord(new ConsumerRecord<>("my_topic", 0,
                2L, "mykey", "myvalue2"));
        mockConsumer.addRecord(new ConsumerRecord<>("my_topic", 0,
                3L, "mykey", "myvalue3"));
        mockConsumer.addRecord(new ConsumerRecord<>("my_topic", 0,
                4L, "mykey", "myvalue4"));

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean();

        StreamConsumer.subscribe(mockConsumer, "my_topic",
                result -> result.catchError(error -> {
                    error.printStackTrace();
                    fail.set(true);
                    result.cancel();
                }).then(consumerRecords -> {
                    System.out.printf("Consumer Records count %d \n", consumerRecords.count());
                    countDownLatch.countDown();
                    result.request(1);
                    result.cancel();
                }), false);

        countDownLatch.await(5, TimeUnit.SECONDS);
        assertFalse(fail.get());
    }

    @Test
    public void subscribeFail() throws Exception {

        final Consumer<String, String> mockConsumer = new Consumer<String, String>() {
            @Override
            public Set<TopicPartition> assignment() {
                return null;
            }

            @Override
            public Set<String> subscription() {
                return null;
            }

            @Override
            public void subscribe(Collection<String> topics) {

            }

            @Override
            public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {

            }

            @Override
            public void assign(Collection<TopicPartition> partitions) {

            }

            @Override
            public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {

            }

            @Override
            public void unsubscribe() {

            }

            @Override
            public ConsumerRecords<String, String> poll(long timeout) {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void commitSync() {

            }

            @Override
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

            }

            @Override
            public void commitAsync() {

            }

            @Override
            public void commitAsync(OffsetCommitCallback callback) {

            }

            @Override
            public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

            }

            @Override
            public void seek(TopicPartition partition, long offset) {

            }

            @Override
            public void seekToBeginning(Collection<TopicPartition> partitions) {

            }

            @Override
            public void seekToEnd(Collection<TopicPartition> partitions) {

            }

            @Override
            public long position(TopicPartition partition) {
                return 0;
            }

            @Override
            public OffsetAndMetadata committed(TopicPartition partition) {
                return null;
            }

            @Override
            public Map<MetricName, ? extends Metric> metrics() {
                return null;
            }

            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                return null;
            }

            @Override
            public Map<String, List<PartitionInfo>> listTopics() {
                return null;
            }

            @Override
            public Set<TopicPartition> paused() {
                return null;
            }

            @Override
            public void pause(Collection<TopicPartition> partitions) {

            }

            @Override
            public void resume(Collection<TopicPartition> partitions) {

            }

            @Override
            public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
                return null;
            }

            @Override
            public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
                return null;
            }

            @Override
            public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public void close(long timeout, TimeUnit unit) {

            }

            @Override
            public void wakeup() {

            }
        };

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean();

        final Stream<ConsumerRecords<String, String>> stream = result -> result.catchError(error -> {
            countDownLatch.countDown();
            result.cancel();
        }).then(consumerRecords -> {
            countDownLatch.countDown();
            fail.set(true);
            result.request(1);
            result.cancel();
        });

        StreamConsumer.subscribe(mockConsumer, "my_topic",
                stream, false);

        stream.reject(new RuntimeException("bad stuff"));
        countDownLatch.await(5, TimeUnit.SECONDS);
        assertFalse(fail.get());
    }

}