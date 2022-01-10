package com.jiangzilong;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:JZL
 * @Date: 2021/12/15  10:45
 * @Version 1.0
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.获取连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启ck并开启状态后端 fs
        env.setStateBackend(new MemoryStateBackend());
        env.enableCheckpointing(5000L); // 多久执行一次ck 两个ck的头跟头之间的间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L); //设置超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);//同时存在最大的ck
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000); // 两个ck的头和尾之间的间隔




        //2.获取连接
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") // 不写表的时候会监控库下面的所有的表,要写表的话必须要写库名+表名
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> stringSource = env.addSource(sourceFunction);

        stringSource.print();

        env.execute("FlinkCDC");


    }
}
