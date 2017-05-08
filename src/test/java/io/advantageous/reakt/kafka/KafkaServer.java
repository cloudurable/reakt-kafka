package io.advantageous.reakt.kafka;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class KafkaServer {

    private final ZooKeeperServerMain zooKeeperServer;
    private final KafkaServerStartable kafkaServer;

    public KafkaServer() {

        final Properties zkProperties = new Properties();
        final Properties kafkaProperties = new Properties();
        try {
            //load properties
            kafkaProperties.load(Class.class.getResourceAsStream("/io/advantageous/reakt/kafka/kafka.properties"));
            zkProperties.load(Class.class.getResourceAsStream("/io/advantageous/reakt/kafka/zookeeper.properties"));
        } catch (Exception e){
            throw new RuntimeException(e);
        }


        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        new Thread(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        }).start();



        //start local kafka broker
        kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProperties));
        kafkaServer.startup();


    }

    public void shutdown() {
        kafkaServer.shutdown();
    }

    public static void main(String[] args) {
        new KafkaServer();
    }
}
