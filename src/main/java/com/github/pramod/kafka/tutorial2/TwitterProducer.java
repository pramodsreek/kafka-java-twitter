package com.github.pramod.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String bootstrapServers = "127.0.0.1:9092";

    List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer(){}

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }

    public void run() throws IOException {
        logger.info("set up");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10);

        //Create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("stopping application ...");
                    logger.info("shutting down client from twitter...");
                    client.stop();
                    logger.info("closing producer...");
                    producer.close();
                    logger.info("done");
                }
        ));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                           logger.error("Something went wrong", e);
                        }
                    }
                });
            }
            logger.info("End of application");
        }
    }

    public KafkaProducer<String, String> createKafkaProducer() {
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

        return producer;

    }

    GetPropertyValues values = new GetPropertyValues();
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;

    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {

        try {
            consumerKey = values.getPropValues("consumerKey");
            consumerSecret = values.getPropValues("consumerSecret");
            token = values.getPropValues("token");
            secret = values.getPropValues("secret");
        } catch (IOException e) {
            logger.error("Twitter keys not found ",e);
            throw e;
        }


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

}
