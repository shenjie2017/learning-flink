package com.blue.flink.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> input = env.fromElements("my name is jason",
                "i like reading book",
                "i hive eating  food",
                "and i like playing stock");
        Schema lineSchema = Schema.newBuilder().column("f0", DataTypes.STRING()).build();
//        Table lines = tEnv.fromDataStream(input, lineSchema);
        Table lines = tEnv.fromDataStream(input).renameColumns($("f0").as("line"));
//        Table lines = tEnv.fromDataStream(input, $("line"));

        lines.printSchema();

//        env.execute();

    }
}
