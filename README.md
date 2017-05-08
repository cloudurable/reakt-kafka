This libray adapts Kafka to [Reakt promises and streams](http://advantageous.github.io/reakt/).


Reakt has promise libraries for Vert.x, Netty, Gauva and Cassandra.



#### Using Promises with Kafka Producers
```java

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