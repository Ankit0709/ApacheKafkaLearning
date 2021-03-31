import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.translog.Translog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.x509.CertificateX509Key;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    private  static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();
        //Get kafka Consumer
        KafkaConsumer<String, String> consumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer size = records.count();
            logger.info("Number Of Records "+size);
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {

                String jsonString = record.value();
                //2 Ways to create Idempotent Consumer
                //(A) Kafka Generic Id
//                String id = record.topic()+"_"+record.partition()+"_"+record.offset();

                //(B)Twitter Tweets Id

                try{
                    //Create IndexRequest
                    String id = extractIdFromJson(record.value());
                    IndexRequest request = new IndexRequest("twitter");
                    request.id(id);
                    request.source(jsonString, XContentType.JSON);
                    bulkRequest.add(request);
                }
                catch(Exception ex){
                    logger.warn("Skipping this Request"+record.value());
                }

            }
            if(size>0){
                BulkResponse response = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Commiting Offsets...");
                consumer.commitSync();
                logger.info("Comitted Offsets");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }
    public static KafkaConsumer<String,String> createConsumer(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-twitter-tweets";
        String topic="twitter_tweets";

        // Create Consumer Properties Configuration
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        //Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromJson(String json){
        return jsonParser.parse(json)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
    public  static RestHighLevelClient createClient(){

        //provide credentails here !
        String hostname = "kafka-twitter-422302935.eu-west-1.bonsaisearch.net";
        String username = "zrfii53yww";
        String password = "jabca4eeg";

        //don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname,443,"https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
