package com.jdes.kafka.twitter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TutorialConsumer {
    // this class will access the ES within bonsai...
    // TwitterElasticConsumer was conncecting to the ES that I dl'd on my lapto

    public static RestHighLevelClient createClient() {

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

        String hostname = propertiez.getProperty("hostname"); // localhost or bonsai url

    String username = propertiez.getProperty("username"); // needed only for bonsai
    String password = propertiez.getProperty("password"); // needed only for bonsai


    // credentials provider help supply username and password
        // only necessary when ES is cloud based b/c of its secure mode
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));

// We're going to connect over HTTP to the hostname...
        // Over port 443 (HTTPS), i.e. encrypted connection
        // The callback says to apply the credentials to any
        // HTTP calls
    RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });

    RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
}

    public static KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers = "127.0.0.1:9092";
        // you can change the groupId to get the data from the beginning of the offsets
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter_tweets";

        // New consumer configs (Kafka docs)
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // "earliest/latest/none"
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(TutorialConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String jsonString = "{ \"foo\": \"bar\" }";

        // index, type, id...
        // this will fail unless the twitter index exists...
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
    String id = indexResponse.getId();
    logger.info(id);

    // close the client gracefully
    client.close();


    }

}
