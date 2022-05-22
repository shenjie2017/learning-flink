package com.blue.mall.data.ods;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsEventTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果key基本相同，需要设置并行度为1来开开发测试，因为水位线是参照并行度最低，
        // 如果设置为多个并行度，可能有的并行度并没有接收到数据，导致窗口无法关闭
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table ods_event( " +
                "id string, " +
                "url string, " +
                "`time` string, " +
                "ts bigint, " + //事件过程时间
                "time_ltz AS TO_TIMESTAMP_LTZ(ts, 3), " + //转换成时间戳
//                "user_action_time AS PROCTIME() "+
                "WATERMARK FOR time_ltz AS time_ltz - INTERVAL '10' SECOND " + //取最近10秒到数据
                ") with ( " +
                "'connector'='kafka', " +
                "'topic'='mall_click_event', " +
                "'properties.group.id'='data_group', " +
                "'properties.bootstrap.servers'='localhost:9092', " +
                "'scan.startup.mode'='latest-offset', " +
                "'value.format'='json' ) ");
//        Table q1 = tEnv.sqlQuery("select * from ods_event ");
//        tEnv.toChangelogStream(q1).print();
        //时间窗口到数据
//        Table q2 = tEnv.sqlQuery("select id,url,`time`,ts,window_start,window_end,window_time from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) ");
//        Table q2 = tEnv.sqlQuery("select * from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) ");
//        tEnv.toDataStream(q2).print();
        //时间窗口内的数据聚合操作
        Table q3 = tEnv.sqlQuery("select window_start,window_end,id,count(1) as cnt from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) group by window_start,window_end,id ");
        tEnv.toChangelogStream(q3).print();
//        Table q4 = tEnv.sqlQuery("select count(1) as cnt from TABLE( TUMBLE( TABLE ods_event ,DESCRIPTOR(time_ltz), INTERVAL '10' SECOND)  ) ");
//        tEnv.toChangelogStream(q4).print();

//        Table q5 = tEnv.sqlQuery("SELECT TUMBLE_START(user_action_time, INTERVAL '10' SECOND),id, COUNT(1) as cnt FROM ods_event GROUP BY TUMBLE(user_action_time, INTERVAL '10' SECOND),id ");
//        Table q5 = tEnv.sqlQuery("SELECT TUMBLE_START(time_ltz, INTERVAL '10' SECOND),id, COUNT(1) as cnt FROM ods_event GROUP BY TUMBLE(time_ltz, INTERVAL '10' SECOND),id ");
//        tEnv.toChangelogStream(q5).print();

//        Table q6 = tEnv.sqlQuery("select * " +
//                "from ( " +
//                "    select window_start,id,cnt,row_number() over(partition by window_start order by cnt desc) as rn " +
//                "    from ( " +
//                "        SELECT TUMBLE_START(time_ltz, INTERVAL '10' SECOND) as window_start,id, COUNT(1) as cnt FROM ods_event GROUP BY TUMBLE(time_ltz, INTERVAL '10' SECOND),id " +
//                "    ) t " +
//                ") t " +
//                "where rn<=3 ");
//        tEnv.toChangelogStream(q6).print();

        env.execute();
    }
}
