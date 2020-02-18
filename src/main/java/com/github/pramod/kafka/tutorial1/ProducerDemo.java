package com.github.pramod.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());

        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic","Hello Kafka!");

        //key value of type string
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // send data - asynchronous
        producer.send(record);

        //flush and close producer
        producer.flush();
        producer.close();
    }
}
