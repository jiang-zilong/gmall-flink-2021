package com.jiangzilong.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.jiangzilong.app.function.CustomerDeserialization;
import com.jiangzilong.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author:JZL
 * @Date: 2021/12/16  14:27
 * @Version 1.0
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        ///1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("hdfs://hadoop111:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        //2.获取连接
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .databaseList("gmall-flink")
                //.tableList("gmall-flink.base_trademark") // 不写表的时候会监控库下面的所有的表,要写表的话必须要写库名+表名
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3.将数据写入到kafka中
        streamSource.print();

        String topic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(topic));




        //4.启动
        env.execute();

    }
}
