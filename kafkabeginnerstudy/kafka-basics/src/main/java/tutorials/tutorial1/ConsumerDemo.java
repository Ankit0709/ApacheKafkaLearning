package tutorials.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    final private  static String bootstrapServer = "127.0.0.1:9092";
    final  private static String topic = "my_first_topic";
    final private static String groupId = "my_second_application";
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        //create  Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //create Consumer Class
        Consumer<String, String> consumer =  new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new Data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                logger.info("Key is:  "+record.key()+" , Value is: "+record.value());
                logger.info("Offset is:  "+record.offset()+" ,  Partition is :"+record.partition());
            }
        }

    }
}
