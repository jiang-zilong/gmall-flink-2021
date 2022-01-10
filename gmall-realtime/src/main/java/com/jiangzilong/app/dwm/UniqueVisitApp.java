package com.jiangzilong.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.jiangzilong.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @Author:JZL
 * @Date: 2021/12/29  13:57
 * @Version 1.0
 * UV
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*      env.setStateBackend(new FsStateBackend("hdfs://hadoop111:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);*/


        //2.读取kafka dwd_page_log数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3.将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //4.过滤  状态编程
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("value-state", String.class);
                //设置状态保存的超时时间
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面的数据
                String lastPageid = value.getJSONObject("page").getString("last_page_id");
                if (lastPageid == null || lastPageid.length() <= 0) {
                    //取出状态
                    String lastDate = dateState.value();
                    //取出今天的日期
                    String curDate = simpleDateFormat.format(value.getLong("ts"));
                    if (!curDate.equals(lastDate)) {
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });
        //5.写入kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //6.启动任务
        env.execute("UniqueVisitApp");

    }

}