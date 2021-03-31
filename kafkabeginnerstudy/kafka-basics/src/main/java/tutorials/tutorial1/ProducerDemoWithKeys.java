package tutorials.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallbacks.class);

        //Create properties
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);




        for(int i=1;i<=10;i++){

            //create producer Record
            String topic = "my_first_topic";
            String value = "Hello World ! This message is from Producer no "+ i;
            String key = "id_"+i;

            //log key
            logger.debug("Key is :"+key+"\n");


            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topic,key,value);
            //to send Data -aschronously either it will send data or generate exception
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Record Meta Data Information"+"\n"+
                                " Topic:  "+recordMetadata.topic()+"\n"+
                                " Partition: "+recordMetadata.partition()+"\n"+
                                " OffSet: "+recordMetadata.offset()+"\n"+
                                " TimeStamp: "+recordMetadata.timestamp()+"\n");
                    }
                    else{
                        logger.error("Error is"+e);
                    }
                }
            }).get();   // to avoid  data .send aschorounsly , do not use in production
        }

        //flush data and close
        producer.flush();
        producer.close();


    }
}
