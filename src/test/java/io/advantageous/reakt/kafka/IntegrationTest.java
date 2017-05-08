package io.advantageous.reakt.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IntegrationTest {

    private final static String TOPIC = "my-test-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static int SEND_RECORD_COUNT = 10_000;


    @Test
    public void test() throws Exception {

        final KafkaServer kafkaServer = new KafkaServer();
        System.out.println("Starting server");
        Thread.sleep(10_000);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final AsyncProducer<Long, String> producer = new AsyncProducer<>(createProducer());

        executorService.execute(() -> {
            for (int i = 0; i < SEND_RECORD_COUNT; i++) {
                if (i % 1000 == 0) System.out.println("Sending message " + i);
                producer.send(TOPIC, 1L * i, "value " + i)
                        .catchError(throwable -> {
                            System.err.println("Trouble sending record " + throwable.getLocalizedMessage());
                            throwable.printStackTrace(System.err);
                        })
                        .then(recordMetadata -> {
                                if (recordMetadata.offset() % 1000 ==0)
                                System.out.printf("%d %d %s \n", recordMetadata.offset(),
                                        recordMetadata.partition(), recordMetadata.topic());
                        })
                        .invoke();
            }
            producer.flush();
        });


        final CountDownLatch countDownLatch = new CountDownLatch(SEND_RECORD_COUNT);

        final StreamConsumer<Long, String> stream = StreamConsumer.subscribe(createConsumer(), TOPIC, result -> {
            result.then(consumerRecords -> {
                System.out.println("Got message " + consumerRecords.count());
                consumerRecords.forEach(record -> {
                    countDownLatch.countDown();
                });
                result.request(1);
            }).catchError(throwable -> {
                System.err.println("Trouble Getting record " + throwable.getLocalizedMessage());
                throwable.printStackTrace(System.err);
                result.cancel();
            });
        });

        Thread.sleep(3_000);

        countDownLatch.await(10, TimeUnit.SECONDS);
        assertEquals(0, countDownLatch.getCount());
        stream.close();
        producer.close();
        executorService.shutdown();

        Thread.sleep(3_000);
        kafkaServer.shutdown();
        Thread.sleep(3_000);

    }


    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return new KafkaConsumer<>(props);
    }
}