package com.blue.flink.demo;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class CustomSinkStream {
    public static class CustomSinkFunction implements SinkFunction<Event>{

        @Override
        public void invoke(Event value, Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            System.out.println("save data: "+value);
        }

        @Override
        public void writeWatermark(Watermark watermark) throws Exception {
            SinkFunction.super.writeWatermark(watermark);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new CustomSourceStream.CustomSourceFunction());

        source.addSink(new CustomSinkFunction());

        env.execute("stream custom sink");
    }
}
