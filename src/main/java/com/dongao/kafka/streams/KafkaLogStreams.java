package com.dongao.kafka.streams;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka 流
 *
 * @author liangchuanchuan
 */
public class KafkaLogStreams {

    public static void main(String[] args) {

        // 1.配置
        Map<String, Object> props = new HashMap<>();
        // 应用名称
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        // kafka集群地址
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        // key,value 序列化
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        // 2.toPo结构
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("SOURCE", "first")
                .addProcessor("PROCESSOR", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new Processor<String, String>() {
                            private ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public void process(String key, String value) {
                                if (value.contains(">>>")) {
                                    context.forward(key, value.split(">>>")[1]);
                                } else {
                                    context.forward(key, value);
                                }
                            }

                            @Override
                            public void punctuate(long timestamp) {

                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                }, "SOURCE")
                .addSink("SINK", "second", "PROCESSOR");

        // 3.start
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, config);
        kafkaStreams.start();
    }

}
