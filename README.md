## Reakt Kafka

This libray adapts [Kafka](http://cloudurable.com/kafka-training/index.html) to [Reakt promises and streams](http://advantageous.github.io/reakt/).


Reakt has promise libraries for Vert.x, Netty, Guava, and Cassandra.



#### Using Promises with Kafka Producers
```java

final AsyncProducer<Long, String> producer = new AsyncProducer<>(createProducer());
...
producer.send(TOPIC, key, value)
    .catchError(throwable -> {
                System.err.println("Trouble sending record " + throwable.getLocalizedMessage());
                     throwable.printStackTrace(System.err);
    })
    .then(recordMetadata -> {
             System.out.printf("%d %d %s \n", recordMetadata.offset(),
                recordMetadata.partition(), recordMetadata.topic());
    }).invoke();

```


#### Using Streams with Kafka Consumers
```java


final StreamConsumer<Long, String> stream = StreamConsumer.subscribe(createConsumer(), TOPIC, result -> {
    result.then(consumerRecords -> {
        System.out.println("Got message " + consumerRecords.count());
        consumerRecords.forEach(record -> {
            countDownLatch.countDown();
        });
        result.request(1); //calls commitAsync
    }).catchError(throwable -> {
        System.err.println("Trouble Getting record " + throwable.getLocalizedMessage());
        throwable.printStackTrace(System.err);
        result.cancel();
    });
});

stream.close();

```
