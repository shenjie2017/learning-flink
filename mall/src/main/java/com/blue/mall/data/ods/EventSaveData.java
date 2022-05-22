package com.blue.mall.data.ods;

import com.alibaba.fastjson.JSON;
import com.blue.mall.data.sink.HDFSUtils;
import com.blue.mall.data.source.KafkaUtils;
import com.blue.mall.data.entry.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventSaveData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用动态路径必须开启checkpoint
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);
        CheckpointConfig chkConf = env.getCheckpointConfig();
        //设置模式为精确一次 (这是默认值)
        chkConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        chkConf.setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        chkConf.setCheckpointTimeout(60000);
        // 允许两个连续的 checkpoint 错误
        chkConf.setTolerableCheckpointFailureNumber(2);
        // 同一时间只允许一个 checkpoint 进行
        chkConf.setMaxConcurrentCheckpoints(1);
        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        chkConf.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        chkConf.enableUnalignedCheckpoints();

        DataStreamSource<String> source = env.fromSource(KafkaUtils.getDefaultSource("mall_click_event", "data_group"),
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "mall_click_event kafka source");

        source.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                return JSON.parseObject(s,Event.class);
            }
        }).print();

        source.addSink(HDFSUtils.getEventDefaultSink("file:///tmp/ods/click_event"));

        env.execute("save data to hdfs");
    }
}
