package com.blue.flink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class CustomSourceStream {
    public static class CustomSourceFunction implements SourceFunction<Event> {
        boolean running=true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            String[] names = {"zhangsan","lisi","wangwu","zhaoliu"};
            String[] urls = {"/home","prod?id=3","prod?id=5","/help","/login"};
            Random rand = new Random();
            while (running){
                ctx.collect(new Event(names[rand.nextInt(names.length)],urls[rand.nextInt(urls.length)],System.currentTimeMillis()));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSourceFunction()).print();

        env.execute("stream rand click data");
    }
}
