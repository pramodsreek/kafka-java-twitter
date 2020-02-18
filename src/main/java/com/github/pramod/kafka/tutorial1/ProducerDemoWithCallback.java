package com.github.pramod.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());

        properties.setProperty("value.serializer", StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            //create the producer

            String topic = "first-topic";
            String value = "Hello Kafka!" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            //by providing a key, the key always goes to a partition.
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key
            //key value of type string
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a recors is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());


                    } else {
                        logger.error("Error while producing", e);

                    }
                }
            }).get(); //block the .send() to make it synchronous - should not be done in production!

            //flush and close producer
            producer.flush();
            producer.close();
        }
    }
}
