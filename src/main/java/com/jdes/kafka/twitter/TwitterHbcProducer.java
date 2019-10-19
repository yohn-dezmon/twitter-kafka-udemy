package com.jdes.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterHbcProducer {

    public static void main(String[] args) throws InterruptedException {

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


        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        // Idk what eventQueue is for...
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // I don't understand what the 'followings' variable does :(
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // in the documentation the examples are "twitter" and "api"
        List<String> terms = Lists.newArrayList("climate change");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client eve

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();

        final Logger logger = LoggerFactory.getLogger(TwitterHbcProducer.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = msgQueue.take();
            String key = "id_" + Integer.toString(msgRead);
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("climatechange",
                    key,msg + Integer.toString(msgRead));

            // send data - asynchronous!
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata.  \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        e.printStackTrace();
                        logger.error("Error while producing", e);
                    }
                }
            });
            System.out.println(key);
            System.out.println(msg);
        }

        hosebirdClient.stop();
        // flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }




    }


