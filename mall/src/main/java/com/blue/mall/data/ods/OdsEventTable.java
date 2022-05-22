package com.blue.mall.data.ods;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsEventTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table ods_event( " +
                "id string, " +
                "url string, " +
                "`time` string, " +
                "ts bigint, " + //事件过程时间
                "time_ltz AS TO_TIMESTAMP_LTZ(ts, 3), " + //转换成时间戳
                "WATERMARK FOR time_ltz AS time_ltz - INTERVAL '10' SECOND " + //取最近10秒到数据
                ") with ( " +
                "'connector'='kafka', " +
                "'topic'='mall_click_event', " +
                "'properties.group.id'='data_group', " +
                "'properties.bootstrap.servers'='localhost:9092', " +
                "'scan.startup.mode'='latest-offset', " +
                "'value.format'='json' ) ");
        Table q1 = tEnv.sqlQuery("select * from ods_event ");
        tEnv.toChangelogStream(q1).print();
        //时间窗口到数据
//        Table q2 = tEnv.sqlQuery("select id,url,`time`,ts from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) ");
//        tEnv.toChangelogStream(q2).print();
        //时间窗口内的数据聚合操作
//        Table q3 = tEnv.sqlQuery("select id,count(1) as cnt from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) group by id ");
//        tEnv.toChangelogStream(q3).print();
//        Table q4 = tEnv.sqlQuery("select count(1) as cnt from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) ");
//        tEnv.toChangelogStream(q4).print();

//        Table q5 = tEnv.sqlQuery("SELECT TUMBLE_START(time_ltz, INTERVAL '10' SECOND),id, COUNT(1) as cnt FROM ods_event GROUP BY TUMBLE(time_ltz, INTERVAL '10' SECOND),id ");
//        tEnv.toChangelogStream(q5).print();

        env.execute();
    }
}
