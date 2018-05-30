package com.dongao.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * first message producer
 *
 * @author liangchuanchuan
 */
public class FirstProducer {

    public static void main(String[] args) {
        // 1.配置属性
        Properties props = new Properties();
        // kafka集群地址
        props.put("bootstrap.servers", "hadoop101:9092");
        // 等待所有副本应答
        props.put("acks", "all");
        // 是否重试
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 服务器请求延迟
        props.put("linger.ms", 1);
        // 发送缓存区内存大小 32M
        props.put("buffer.memory", 33554432);
        // key,value 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // set partition
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.dongao.kafka.partition.OnePartition");
        // set interceptors
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("com.dongao.kafka.interceptor.TimeInterceptor","com.dongao.kafka.interceptor.CountInterceptor"));

        // 2.get produce
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 3.send
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("second", Integer.toString(i)),
                    ((metadata, exception) -> System.out.println(metadata.toString() + "\t" + metadata.partition()))
            );
        }

        producer.close();
    }

}