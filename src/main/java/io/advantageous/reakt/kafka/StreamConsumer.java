package io.advantageous.reakt.kafka;

import io.advantageous.reakt.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Adapts a Kafka Consumer to a Reakt stream.
 * @param <K>
 * @param <V>
 */
public class StreamConsumer<K, V> implements Closeable {

    private final Consumer<K, V> consumer;
    private final List<String> topics;
    private final long pollTime;
    private final Stream<ConsumerRecords<K, V>> stream;
    private final ExecutorService executorService;
    private final Runnable cancelHandler;
    private final Runnable emptyHandler;
    private final java.util.function.Consumer<Long> wantsMoreConsumer;
    private final boolean subscribe;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Duration forceStopDuration;
    private AtomicBoolean done = new AtomicBoolean();
    private AtomicBoolean wantsMore = new AtomicBoolean();

    public StreamConsumer(final Consumer<K, V> consumer, final List<String> topics,
                          final Duration pollTime,
                          final Stream<ConsumerRecords<K, V>> stream,
                          final ExecutorService executorService,
                          final Runnable cancelHandler,
                          final Runnable emptyHandler,
                          final java.util.function.Consumer<Long> wantsMoreConsumer,
                          final boolean subscribe,
                          final ScheduledExecutorService scheduledExecutorService,
                          final Duration forceStopDuration) {

        this.subscribe = subscribe;
        this.consumer = consumer;
        this.topics = Collections.unmodifiableList(topics);
        this.pollTime = pollTime.toMillis();
        this.stream = stream;
        this.executorService = executorService;
        this.forceStopDuration = forceStopDuration;


        if (wantsMoreConsumer != null) {
            this.wantsMoreConsumer = wantsMoreConsumer;
        } else {
            this.wantsMoreConsumer = more -> {
                if (more > 0) wantsMore.set(true);
            };
        }

        if (cancelHandler != null) {
            this.cancelHandler = cancelHandler;
        } else {
            this.cancelHandler = () -> done.set(true);
        }


        if (emptyHandler != null) {
            this.emptyHandler = emptyHandler;
        } else {
            this.emptyHandler = () -> {
            };
        }

        if (scheduledExecutorService != null) {
            this.scheduledExecutorService = scheduledExecutorService;
        } else {
            this.scheduledExecutorService = Executors.newScheduledThreadPool(2);
        }
    }


    public StreamConsumer(final Consumer<K, V> consumer, final List<String> topics,
                          final Duration pollTime,
                          final Stream<ConsumerRecords<K, V>> stream,
                          final ExecutorService executorService,
                          final boolean subscribe) {

        this(consumer, topics, pollTime, stream, executorService, null,
                null, null, subscribe, null, Duration.ofSeconds(3));
    }


    /**
     * Subscribe to a stream
     * @param consumer Kafka consumer
     * @param topic topic
     * @param stream stream
     * @return StreamConsumer with close method and commitAsync.
     */
    public static <K, V> StreamConsumer<K, V> subscribe(final Consumer<K, V> consumer,
                                                        final String topic,
                                                        final Stream<ConsumerRecords<K, V>> stream) {

        return subscribe(consumer, topic, Duration.ofMillis(100), stream, Executors.newSingleThreadExecutor(), true);
    }

    public static <K, V> StreamConsumer<K, V> subscribe(final Consumer<K, V> consumer,
                                                        final String topic,
                                                        final Stream<ConsumerRecords<K, V>> stream,
                                                        final boolean subscribe) {

        return subscribe(consumer, topic, Duration.ofMillis(100), stream, Executors.newSingleThreadExecutor(), subscribe);
    }

    public static <K, V> StreamConsumer<K, V> subscribe(final Consumer<K, V> consumer,
                                                        final String topic,
                                                        final Duration pollTime,
                                                        final Stream<ConsumerRecords<K, V>> stream,
                                                        final ExecutorService executorService,
                                                        final boolean subscribe) {

        final StreamConsumer streamConsumer = new StreamConsumer<K, V>(consumer, Collections.singletonList(topic),
                pollTime, stream, executorService, subscribe);

        streamConsumer.subscribe();

        return streamConsumer;
    }

    private void subscribe() {

        if (subscribe) consumer.subscribe(topics);

        executorService.submit(() -> {
            while (!done.get()) {
                try {
                    final ConsumerRecords<K, V> consumerRecords = consumer.poll(pollTime);
                    if (consumerRecords.count() == 0) {
                        emptyHandler.run();
                    } else {
                        stream.reply(consumerRecords, done.get(), cancelHandler, wantsMoreConsumer);
                        if (wantsMore.get()) {
                            consumer.commitAsync();
                            wantsMore.set(false);
                        }
                    }
                } catch (Exception ex) {
                    stream.reject(ex);
                }
            }

            if (done.get()) {
                scheduledExecutorService.schedule(() -> {
                    try {
                        consumer.close();
                    } finally {
                        executorService.shutdown();
                    }
                }, this.forceStopDuration.toMillis(), TimeUnit.MILLISECONDS);
            }

        });

    }

    @Override
    public void close() {
        done.set(true);
    }

    public void commitAsync() {
        consumer.commitAsync();
    }

    public void commitSync() {
        consumer.commitSync();
    }


    public Consumer<K, V> getConsumer() {
        return consumer;
    }
}
