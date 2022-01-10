package com.jiangzilong.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.jiangzilong.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author:JZL
 * @Date: 2021/12/16  17:01
 * @Version 1.0
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        env.setStateBackend(new FsStateBackend("hdfs://hadoop111:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);*/


        //TODO 2.消费ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为json对象
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {

        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常，将数据写入到侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });

        //TODO 打印脏数据
        jsonObjDS.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>>>>>");


        //TODO 4.新老用户校验 状态编程

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext().getState(new ValueStateDescriptor<String>("value-staate", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //获取数据中的is_new表示
                        String isNew = value.getJSONObject("common").getString("is_new");
                        //判断is_new标记是否是1
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("0");
                            }
                        }
                        return value;
                    }
                });

        //TODO 5.分流  侧输出流  页面：主流   启动：侧输出流    曝光：侧输出流

        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取启动日志字段
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入到启动侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    out.collect(value.toJSONString());
                    //取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("display");
                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            ctx.output(displayTag, value.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //TODO 7.将三个流打印到kafka的主题中
        startDS.print("Start>>>>>>>>>>>>>>>");
        pageDS.print("PageDS>>>>>>>>>>>>>>>");
        displayDS.print("DisplayDS>>>>>>>>>>>>>>>>>");


        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动
        env.execute("BaseLogApp");

    }
}
