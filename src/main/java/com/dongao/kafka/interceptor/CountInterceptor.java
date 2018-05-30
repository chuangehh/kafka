package com.dongao.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * count 拦截器
 *
 * @author liangchuanchuan
 */
public class CountInterceptor implements ProducerInterceptor {

    private AtomicInteger success = new AtomicInteger();

    private AtomicInteger fail = new AtomicInteger();


    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success.incrementAndGet();
        } else {
            fail.incrementAndGet();
        }
    }

    @Override
    public void close() {
        System.out.println("success:" + success.get() + "fail:" + fail.get());
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
