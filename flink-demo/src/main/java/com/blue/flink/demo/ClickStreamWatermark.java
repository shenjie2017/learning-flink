package com.blue.flink.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class ClickStreamWatermark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //socket默认单并行度
        //如果key基本相同，需要设置并行度为1来开开发测试，因为水位线是参照并行度最低，
        // 如果设置为多个并行度，可能有的并行度并没有接收到数据，导致窗口无法关闭
        env.setParallelism(1);

        DataStreamSource<String> line = env.socketTextStream("localhost", 9999);
        DataStream<Event> watermarkDS = line.map(new MapFunction<String, Event>() {

            @Override
            public Event map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Event(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //水位线生成策略，时间戳前n秒
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.ts;
                    }
                }));

        OutputTag<Event> lateTag = new OutputTag<Event>("late-data"){};
        OutputTag<Event> warnTag = new OutputTag<Event>("warn-data"){};
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(event -> event.id)
                //窗口类型和窗口大小
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //水位线之外允许延迟多少时间
                .allowedLateness(Time.seconds(2))
                //延迟之外的迟到数据给个标签
                .sideOutputLateData(lateTag)
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        Long cnt = 0L;


                        for (Event ele : elements) {
                            cnt++;
                            if("wu".equals(s)){
                                context.output(warnTag,ele);
                            }
                        }

                        out.collect("window_start:" + context.window().getStart() + " window_end: " + context.window().getEnd() + " data key: " + s + " data value: " + cnt);                       ;
                    }
                });

        result.print("result: ");


        result.getSideOutput(lateTag).print("late-data: ");
        result.getSideOutput(warnTag).print("warn-data: ");

        env.execute("execute word count stream watermark");
    }
}
