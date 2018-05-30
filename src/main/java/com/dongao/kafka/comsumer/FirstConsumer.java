package com.dongao.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者
 *
 * @author liangchuanchuan
 */
public class FirstConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka集群address
        props.put("bootstrap.servers", "hadoop101:9092");
        // 消费组
        props.put("group.id", "test");
        // 开启zookeeper自动管理offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        // 订阅两个分区
        consumer.subscribe(Arrays.asList("first", "second"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
