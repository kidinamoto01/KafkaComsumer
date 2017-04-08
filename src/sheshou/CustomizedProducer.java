package sheshou;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by b on 17/4/8.
 */
public class CustomizedProducer {
    Producer<String, String> producer ;
    static KafkaConsumer<String, String> consumer;

    int iKey =0;
    public void initial(){
        //确认Consumer的属性
        Properties comsumerProps = new Properties();
        comsumerProps.put("bootstrap.servers", "localhost:9092");
        comsumerProps.put("group.id", "group1");
        comsumerProps.put("enable.auto.commit", "false");
        comsumerProps.put("session.timeout.ms", "30000");
        comsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        comsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //初始化Producer的属性
        // create instance for properties to access producer configs
        Properties producerProps = new Properties();
        //Assign localhost id
        producerProps.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        producerProps.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        producerProps.put("retries", 0);
        //Specify buffer size in config
        producerProps.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        producerProps.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(producerProps);
        consumer = new KafkaConsumer<String, String>(comsumerProps);
        //接受信息的Topic
        SetReceiveTopic("test");
    }
    public void SendMessage(String strMsg,String topicName){
        producer.send(new ProducerRecord<String, String>(topicName,
                Integer.toString(iKey), strMsg));
        System.out.println("***Msg: "+strMsg+" offset "+iKey);
        iKey++;
    }
    public void SetReceiveTopic(String name){
        consumer.subscribe(Arrays.asList(name));
    }

    /*
    * 将字符串通过分隔符拆分；然后将相关字段生成变量
    * */
    public TargetClass TransformMessage(String input){

        String[] parts = input.split("-");

        TargetClass t = new TargetClass(parts[1],parts[2],parts[3],parts[4]);
        return t;
    }

    public static void main(String[] args) throws Exception{

        CustomizedProducer sample = new CustomizedProducer();
        //初始化
        sample.initial();
        //接收消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);


            for (TopicPartition partition : records.partitions()) {

                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                for (ConsumerRecord<String, String> record : partitionRecords) {

                    System.out.println("Thread = " + Thread.currentThread().getName() + " ");
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
                    System.out.println("\n");
                }
                // consumer.commitSync();
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
    }
    class TargetClass{
        private String type; //类型
        private String ip1; //源IP
        private String ip2; //目标IP
        private String eventTime; //发生时间
        public TargetClass(String str1, String str2,String str3,String str4){
            this.type=str1;
            this.ip1= str2;
            this.ip2 = str3;
            this.eventTime = str4;
        }
    }
}
