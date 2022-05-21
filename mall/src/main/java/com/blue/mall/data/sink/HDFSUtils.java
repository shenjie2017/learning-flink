package com.blue.mall.data.sink;

//import com.alibaba.fastjson.JSON;
//import com.blue.mall.data.entry.Event;
//import org.apache.flink.streaming.connectors.fs.Clock;
//import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
//import org.apache.flink.streaming.connectors.fs.Writer;
//import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
//import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
//import org.apache.hadoop.fs.Path;
////import org.apache.hadoop.fs.Path;
//
//import java.io.File;
//import java.io.IOException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class HDFSUtils {
    public static StreamingFileSink<String> getEventDefaultSink(String path){
//        BucketingSink<String> sink = new BucketingSink<>(path);
//
//        sink.setBucketer(new Bucketer<String>() {
//
//            @Override
//            public Path getBucketPath(Clock clock, Path path, String s) {
//
//                String ymd = JSON.parseObject(s, Event.class).getTime().substring(0,10);
//                return new Path(path.toString()+ File.separator+ymd);
//            }
//        });
//        1G
//        sink.setBatchSize(1024*1024*1024);
//        20 mins
//        sink.setBatchRolloverInterval(20*60*1000);
//
//        return sink;

        StreamingFileSink<String> build = StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        //时长 滚动切割1h
                        .withRolloverInterval(Duration.ofHours(1))
                        //空闲 滚动切割 1min
                        .withInactivityInterval(Duration.ofMinutes(1))
                        //大小 滚动切割 1g
                        .withMaxPartSize(MemorySize.ofMebiBytes(1024*1024*1024))
                        .build())
                //自定义字段保存目录
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String s, Context context) {
                        JSONObject json = JSON.parseObject(s);
//                        String ymd = json.getString("time").substring(0, 10);
                        long ts = json.getLong("ts");
                        String ymd = new Timestamp(ts).toString().substring(0, 10).replace("-", "");
                        return "dt=" + ymd;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("clickEvent")
                        .withPartSuffix(".txt")
                        .build())
                .withBucketCheckInterval(TimeUnit.SECONDS.toMillis(10))
                .build();
        return build;
    }
}
