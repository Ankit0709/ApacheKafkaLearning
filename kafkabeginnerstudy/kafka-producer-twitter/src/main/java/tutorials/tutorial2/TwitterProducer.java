package tutorials.tutorial2;

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final String consumerKey = "eR11arUjBirsnuAqYoHRjihih";
    private final String consumerSecret  = "cI920zYEjFbbiiDRR5ChwbBxe1SDqVI3v3KeR1Zg3JLQSN4DfV";
    private final String token = "759375398220201985-R6Zwrth3appb56R5TsPy7Kdnzffe3p4";
    private final String secret  = "m4LR9UQM0zPTjvhtbXoSJPszcZYG0HaVxdspGrA1xHenp";


    private final String bootstrapServer = "127.0.0.1:9092";
    private final String topic = "twitter_tweets";
    private final String value = "Hey ! I am learning kafka With Twitter";
    List<String> terms = Lists.newArrayList("bitcoin","usa","politics","india");


    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    TwitterProducer(){}

    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        //1. Create a Twitter Client
        Client client = createTwitterClient(msgQueue);
        client.connect();
        logger.info("Connected To Twitter!");

        //2. Create a Kafka Producer
        KafkaProducer<String,String> producer = createKafkaProducer();


        //add shutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping Application");
            logger.info("Stopping Client");
            client.stop();
            logger.info("Stopping Producer");
            producer.close();
            logger.info("Done !!!");
        }));


        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg !=  null){
                logger.info(msg);
                producer.send(new ProducerRecord<>(topic, null, msg),new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Something Bad has happened",e);
                        }
                    }
                });
            }
        }

        //3. Loop through to send Twitter Tweets
    }
    public KafkaProducer<String, String> createKafkaProducer(){
        //(a) Create Kafka Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create safe Producer By setting Idempotent Producer true ,acks =all ,retry config to INT_MAX,
        // request per connection to 5
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); //IF kafka >=1.0 then 5 otherwise 1

        //Increase Throughput of Producer (at the expense of latency and CPU utilisation)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //(b) Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret, token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
