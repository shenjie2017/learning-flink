package com.blue.mall.data.mock;


import com.alibaba.fastjson.JSON;
import com.blue.mall.data.entry.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;

public class EventKafkaProducer {
    private static boolean running = true;

    private static Event randClickEvent(String[] ids,String[] urls){
        Random random = new Random();
        String id = ids[random.nextInt(ids.length)];
        String url = urls[random.nextInt(urls.length)];

        long ts = System.currentTimeMillis();
        String time = new Timestamp(ts).toString();
        return new Event(id,url,time,ts);
    }

    private static Event randClickEvent(){
        String[] ids = {"kevin","lili","bob","joe"};
        String[] urls = {"/home","/help","/prod?id=1","/prod?id=20","/prod?id=455","/login","/error"};
        return randClickEvent(ids,urls);
    }

    public static void main(String[] args) throws InterruptedException {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers","127.0.0.1:9092");
        conf.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        conf.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        String topic = "mall_click_event";
        KafkaProducer producer = new KafkaProducer(conf);

        while (running){
            Event event = randClickEvent();
            String key = event.getId();
            String data = JSON.toJSONString(event);
            System.out.println(data);
            producer.send(new ProducerRecord(topic,key,data));
            Thread.sleep(500);
        }
        producer.close();
    }
}
