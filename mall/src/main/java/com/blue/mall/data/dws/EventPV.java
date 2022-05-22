package com.blue.mall.data.dws;

import com.alibaba.fastjson.JSON;
import com.blue.mall.data.entry.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class EventPV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.fromSource(KafkaSource.<String>builder()
                        .setBootstrapServers("127.0.0.1:9092")
                        .setTopics("mall_click_event")
                        .setGroupId("data_group")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO), "event pv");

        SingleOutputStreamOperator<Event> data = input.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                return JSON.parseObject(s, Event.class);
            }
        }).map(new MapFunction<Event, Event>() {
            @Override
            public Event map(Event event) throws Exception {
                String url = event.getUrl().split("\\?")[0];
                event.setUrl(url);
                return event;
            }
        });

//        SingleOutputStreamOperator<String> upvCount =
        SingleOutputStreamOperator<Tuple3<String, String, Long>> upvCount = data.keyBy(new KeySelector<Event, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Event event) throws Exception {
                        return Tuple2.of(event.getId(), event.getUrl());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                               @Override
                               public Tuple3<String, String, Long> createAccumulator() {
                                   return new Tuple3<>(null, null, 0L);
                               }

                               @Override
                               public Tuple3<String, String, Long> add(Event event, Tuple3<String, String, Long> tuple) {
                                   System.out.println("event: " + event);
                                   return new Tuple3<>(event.getId(), event.getUrl().split("\\?")[0], tuple.f2 + 1);
                               }

                               @Override
                               public Tuple3<String, String, Long> getResult(Tuple3<String, String, Long> tuple) {
                                   return tuple;
                               }

                               @Override
                               public Tuple3<String, String, Long> merge(Tuple3<String, String, Long> t1, Tuple3<String, String, Long> acc1) {
                                   return new Tuple3<>(t1.f0, t1.f1, t1.f2 + acc1.f2);
                               }
                           }
//                           ,
//                        new ProcessWindowFunction<Tuple3<String, String, Long>, String, Tuple2<String, String>, TimeWindow>() {
//                            @Override
//                            public void process(Tuple2<String, String> tuple,
//                                                ProcessWindowFunction<Tuple3<String, String, Long>, String, Tuple2<String, String>, TimeWindow>.Context ctx,
//                                                Iterable<Tuple3<String, String, Long>> iterable,
//                                                Collector<String> collector) throws Exception {
//                                TimeWindow window = ctx.window();
//                                collector.collect("time start: " + window.getStart() + " time end: " + window.getEnd() + " key:" + tuple + " value: " + iterable.toString());
//
//                            }
//                        }
                );
        upvCount.print("user view page result: ");

        SingleOutputStreamOperator<Tuple2<String, Long>> uvPageCount = upvCount.keyBy(tuple -> tuple.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new AggregateFunction<Tuple3<String, String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> createAccumulator() {
                                return Tuple2.of(null,0L);
                            }

                            @Override
                            public Tuple2<String, Long> add(Tuple3<String, String, Long> t3, Tuple2<String, Long> t2) {
                                return Tuple2.of(t3.f0,t3.f2+t2.f1);
                            }

                            @Override
                            public Tuple2<String, Long> getResult(Tuple2<String, Long> t2) {
                                return t2;
                            }

                            @Override
                            public Tuple2<String, Long> merge(Tuple2<String, Long> t2, Tuple2<String, Long> acc1) {
                                return Tuple2.of(t2.f0,t2.f1+acc1.f1);
                            }
                        });
        uvPageCount.print("user view result :");
        SingleOutputStreamOperator<Tuple2<String, Long>> pvUserCount = upvCount.keyBy(tuple -> tuple.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple3<String, String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of(null,0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple3<String, String, Long> t3, Tuple2<String, Long> t2) {
                        return Tuple2.of(t3.f1,t3.f2+t2.f1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> t2) {
                        return t2;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> t2, Tuple2<String, Long> acc1) {
                        return Tuple2.of(t2.f0,t2.f1+acc1.f1);
                    }
                });
        pvUserCount.print("page view users result: ");

        SingleOutputStreamOperator<Tuple2<String, Long>> pv = uvPageCount.keyBy(tuple -> "pv")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>,Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("pv", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple2<String, Long> left, Tuple2<String, Long> right) {
                        return Tuple2.of(right.f0, left.f1 + right.f1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> t) {
                        return t;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> left, Tuple2<String, Long> acc1) {
                        return Tuple2.of(acc1.f0, left.f1 + acc1.f1);
                    }
                });
        pv.print("pv: ");

        SingleOutputStreamOperator<Tuple2<String, Long>> uv = uvPageCount.keyBy(tuple -> "uv")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>,Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("uv", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Tuple2<String, Long> left, Tuple2<String, Long> right) {
                        return Tuple2.of(right.f0, 1 + right.f1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> t) {
                        return t;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> left, Tuple2<String, Long> acc1) {
                        return Tuple2.of(acc1.f0, left.f1 + acc1.f1);
                    }
                });
        uv.print("uv: ");
        env.execute();
    }
}
