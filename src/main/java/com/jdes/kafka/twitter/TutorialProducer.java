package com.jdes.kafka.twitter;

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

import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TutorialProducer {

    Logger logger = LoggerFactory.getLogger(TutorialProducer.class.getName());
    List<String> terms = Lists.newArrayList("kafka");

    public TutorialProducer() {}


    public static void main(String[] args) {
        new TutorialProducer().run();

    }

    public void run() {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();


        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            // by closing the producer this makes the producer send all of the msgs it still has in memory
            // to kafka
            logger.info("closing producer...");
            producer.close();
            logger.info("Done!");
        }));


        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                // the Callback is for intercepting errors! :D
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       // if there is an error...
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });

            }

        }
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {



        Properties propertiez = new Properties();

        try (FileReader reader = new FileReader("config")) {
            propertiez.load(reader);

        } catch (Exception e) {
            e.printStackTrace();
        }

        String consumerKey = propertiez.getProperty("consumerKey");
        String consumerSecret = propertiez.getProperty("consumerSecret");
        String token = propertiez.getProperty("token");
        String tokenSecret = propertiez.getProperty("tokenSecret");

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // This is for following people!
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // in the documentation the examples are "twitter" and "api"

//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-02")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public KafkaProducer<String, String> createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create SAFE producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");



        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }


}