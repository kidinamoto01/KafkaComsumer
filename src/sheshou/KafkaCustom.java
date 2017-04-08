package sheshou;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

/**
 * Created by b on 17/4/8.
 */
public class KafkaCustom {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));
        boolean isRunning = true;
        /* (isRunning) {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(1000);
           if(null != records) {
               System.out.println("not empty");
               process(records);
           }

        }*/
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

   /* private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<byte[], byte[]>> records) {
        Map<TopicPartition, Long> processedOffsets = new HashMap<>();
        for (Map.Entry<String, ConsumerRecords<byte[], byte[]>> recordMetadata : records.entrySet()) {
            List<ConsumerRecord<byte[], byte[]>> recordsPerTopic = recordMetadata.getValue().records();
            for (int i = 0; i < recordsPerTopic.size(); i++) {
                ConsumerRecord<byte[], byte[]> record = recordsPerTopic.get(i);
                // process record
                try {
                    processedOffsets.put(record.topicAndPartition(), record.offset());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return processedOffsets;
    }*/
}
