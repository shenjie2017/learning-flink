package com.blue.mall.data.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaUtils {
    public static KafkaSource<String> getDefaultSource(String topic, String groupId){
        return KafkaSource.<String>builder().
                setBootstrapServers("127.0.0.1:9092")
                .setTopics(topic).
                setGroupId(groupId).
                setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
