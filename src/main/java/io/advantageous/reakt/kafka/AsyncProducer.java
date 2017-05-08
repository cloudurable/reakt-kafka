package io.advantageous.reakt.kafka;

import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.io.Flushable;


/**
 * Adapts a KafkaProducer to a Producers that supports Reakt promises.
 */
public class AsyncProducer<K, V> implements Closeable, Flushable{


    private final Producer<K, V> producer;

    /**
     * Take a Kafka producers and support a Reakt Async Producers that supports promises.
     * @param producer Kafka producer.
     */
    public AsyncProducer(Producer<K, V> producer) {
        this.producer = producer;
    }

    /**
     * Send a record.
     * @param record Record to send.
     * @return invokeable promise.
     */
    public Promise<RecordMetadata> sendRecord(ProducerRecord<K, V> record) {
        return Promises.invokablePromise(promise -> {
                    producer.send(record, (metadata, exception) -> {
                        if (metadata != null) {
                            promise.resolve(metadata);
                        }  else {
                            promise.reject(exception);
                        }
                    });
                }
        );
    }

    /**
     * Send data via topic, key and value.
     * @param topic topic
     * @param key key
     * @param value value
     * @return invokeable promise.
     */
    public Promise<RecordMetadata> send(final String topic, final K key, final V value) {
        return sendRecord(new ProducerRecord<K, V>(topic, key, value));
    }

    /**
     * Producer this class wraps.
     * @return producer.
     */
    public Producer<K, V> getProducer() {
        return producer;
    }

    /**
     * Close the producer.
     */
    @Override
    public void close()  {
        producer.close();
    }

    /**
     * Flush the producer.
     */
    @Override
    public void flush()  {
        producer.close();
    }


}
