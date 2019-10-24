package com.jdes.kafka.twitter;


import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TwitterElasticConsumer {
//    private RestHighLevelClient client;

    public static RestHighLevelClient createClient() {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

    return client;
    }

    public static void main(String[] args) throws IOException {

        new TwitterElasticConsumer().run();


    }

    private TwitterElasticConsumer() {

    }

    private void run() {
        // if you miss the tab for the class, you can get back to that
        // drop down menu with alt+Tab


        Logger logger = LoggerFactory.getLogger(TwitterElasticConsumer.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-twitter-application";
        String topic = "climatechange";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable thread
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            // New consumer configs (Kafka docs)
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            // "earliest/latest/none"
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



            consumer = new KafkaConsumer<String, String>(properties);


            // subscribe consumer to our topic(s)
//        consumer.subscribe(Collections.singleton("first_topic"));
            consumer.subscribe(Arrays.asList(topic));


        }

        @Override
        public void run() {
            RestHighLevelClient client = createClient();
            try {
                while (true) {
                    // set language to 8
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0


                    // the consumer will read all of the data from one partition, then move onto another partiton
                    // unless you have a producer with a KEY, in which case messages will be read in chronological order
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                        // Do I need to create the index manually with Elasticsearch before doing this?
                        // I think this should automatically create the climatechange index...
                        IndexRequest request = new IndexRequest(
                                "climatechangetwitter",
                                "tweets"
                                );
                        request.id(record.key());
                        //record.key()
//                        String jsonString = record.value();
                        String jsonString = "{ \"foo\": \"bar\" }";
                        request.source(jsonString, XContentType.JSON);

                        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                        String id = indexResponse.getId();
                        logger.info(id);


                        // INDEX RESPONSE
//                        try {
//                            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
//                            String index = indexResponse.getIndex();
//                            String type = indexResponse.getType();
//                            String id = indexResponse.getId();
//                            long version = indexResponse.getVersion();
//                            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
//                                logger.info("Data Inserted into: "+ "climatechange index");
//
//                            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
//                                logger.info("Data already exists for ${id}, so it has been updated");
//                            }
//                            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
//                            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//                                logger.info("ShardInfo getSuccessful...?");
//
//                            }
//                            if (shardInfo.getFailed() > 0) {
//                                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
//                                    String reason = failure.reason();
//                                }
//                            }
//                        } catch (ElasticsearchException e) {
//
//                            if (e.status() == RestStatus.CONFLICT) {
//                                e.printStackTrace();
//                            }
//                        }



                    }
                }
            } catch (WakeupException | IOException e) {
                logger.info("Received shutdown signal!");
            } finally {
                // Super important!
                consumer.close();
                // this is probably not the correct place to put this ...
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // tell our main code that we're done with the consumer
                latch.countDown();
            }

        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();

        }

    }
}
