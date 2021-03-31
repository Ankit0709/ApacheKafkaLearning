package tutorials.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithSeekAndAssign {
    public static void main(String[] args) {

         String bootstrapServer = "127.0.0.1:9092";
         String topic = "my_first_topic";

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        //create  Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //create Consumer Class
        Consumer<String, String> consumer =  new KafkaConsumer<String, String>(properties);

        //assign the topic
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 1);
        long offSetToReadFrom = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offSetToReadFrom);
        int noOfMessagesToRead = 5;
        int noOfMessagesRead =0;
        boolean readingPhase = true;
        //poll for new Data
        while(readingPhase){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                noOfMessagesRead++;
                logger.info("Key is:  "+record.key()+" , Value is: "+record.value());
                logger.info("Offset is:  "+record.offset()+" ,  Partition is :"+record.partition());
                if(noOfMessagesRead == noOfMessagesToRead){
                    readingPhase = false;
                    break;
                }
            }
        }
        logger.info("Exiting Application!");

    }
}

