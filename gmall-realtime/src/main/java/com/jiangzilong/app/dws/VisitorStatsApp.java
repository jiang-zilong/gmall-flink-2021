package com.jiangzilong.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jiangzilong.bean.VisitorStats;
import com.jiangzilong.utils.ClickHouseUtil;
import com.jiangzilong.utils.DateTimeUtil;
import com.jiangzilong.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Author:JZL
 * @Date: 2022/1/4  19:28
 * @Version 1.0
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*      env.setStateBackend(new FsStateBackend("hdfs://hadoop111:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);*/

        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        //2. TODO 读取kafka数据创建流
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //3. TODO 将每个流处理成相同的数据类型
        // 3.1 处理uv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        // 3.1 处理uj数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });


        // 3.1 处理pv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            //获取页面信息
            String last_page_id = page.getString("last_page_id");
            long sv = 0;
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "", common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //4.TODO  union
        DataStream<VisitorStats> unionDS = visitorStatsWithPvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        //5.TODO 提取时间戳生成WM
        SingleOutputStreamOperator<VisitorStats> visitorStatsWirhWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //6.TODO 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> KeyedStream = visitorStatsWirhWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc());
            }
        });

        //7.TODO  开窗聚合 10s滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();
                VisitorStats visitorStats = input.iterator().next();
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(end)));
                out.collect(visitorStats);
            }
        });

        result.print();
        //8.TODO  将数据写入ck

        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats_2021 " +
                " values(?,?,?,?,?,?,?,?,?,?,?,?);"));

        //9.TODO 任务启动
        env.execute();

    }
}
