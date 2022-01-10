package com.jiangzilong.app.dwd;

import com.alibaba.fastjson.JSON;
import javax.annotation.Nullable;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.jiangzilong.app.function.CustomerDeserialization;
import com.jiangzilong.app.function.DimSinkFunction;
import com.jiangzilong.app.function.TableProcessFunction;
import com.jiangzilong.bean.TableProcess;
import com.jiangzilong.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;


//数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
//程  序：           mockDb -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase,zk,hdfs)


/**
 * @Author:JZL
 * @Date: 2021/12/20  15:16
 * @Version 1.0
 */
public class BaseDBApp {
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

        //TODO 2.消费kafka中ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupID = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupID));

        //TODO 3.将每行的数据转换为json对象并过滤(delete数据)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出删除的数据
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //TODO 4.使用flinkcdc消费配置表并且处理成  广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state", String.class, TableProcess.class
        );
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6.分流  处理数据    广播流数据，主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //TODO 7.提取Kafka和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        //TODO 8.将kafka数据写入kafka主题，HBase写入phoenix
        kafka.print("kafka>>>>>");
        hbase.print("hbase>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }

        }));


        //TODO 9.启动任务
        env.execute();

    }
}
