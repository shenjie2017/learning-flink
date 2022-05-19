package com.blue.mall.data.mock;

import com.alibaba.fastjson.JSON;
import com.blue.mall.data.entry.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EventKafkaConsumer {
    private static boolean running = true;

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "127.0.0.1:9092");
        conf.setProperty("group.id", "test_consumer");
        conf.setProperty("enable.auto.commit", "true");
        conf.setProperty("auto.offset.reset", "earliest");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        String topic = "mall_click_event";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Collections.singleton(topic));

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String data = record.value();
                Event event = JSON.parseObject(data, Event.class);
                System.out.println("key: " + key + " value: " + event);
            }
        }
        consumer.close();
    }
}
