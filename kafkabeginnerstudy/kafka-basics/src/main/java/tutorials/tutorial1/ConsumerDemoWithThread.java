package tutorials.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "my_second_topic";
        String groupId = "my_second_application";
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        log.info("Creating the consumer Thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                                    bootstrapServer,
                                    groupId,
                                    topic,
                                    latch);

        //start the thread
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        //add a ShutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                log.info("Application has Exited !");
            }
        }

        ));


        try {
            latch.await();
        } catch (InterruptedException e) {
            log.info("Application is Interrupted",e);
        }
        finally {
            log.info("Application is closing");
        }
    }
    public class ConsumerRunnable implements  Runnable{
        private CountDownLatch latch;
        private  Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);
        private Consumer<String,String> consumer;

        private ConsumerRunnable(String bootstrapServer,
                                 String groupId,
                                 String topic,
                                 CountDownLatch latch){

            this.latch = latch;
            //1.Create Consumer Config Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //2.Create a Consumer
            consumer=new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                    for(ConsumerRecord<String,String> record: records){
                        log.info("Key is:  "+record.key()+" , Value is: "+record.value());
                        log.info("Offset is:  "+record.offset()+" ,  Partition is :"+record.partition());
                    }
                }
            }
            catch(WakeupException ex) {
                log.info("Recieved Shutdown Signal ! ");
            }
            finally {
                consumer.close();
                //tells our code we are done with the consumer
                latch.countDown();
            }

        }
        public void shutdown(){
            //to interupt code flow and throw an exception !
            consumer.wakeup();
        }
    }

}
