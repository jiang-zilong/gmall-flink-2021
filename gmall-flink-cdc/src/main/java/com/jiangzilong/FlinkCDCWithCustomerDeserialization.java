package com.jiangzilong;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:JZL
 * @Date: 2021/12/15  10:45
 * @Version 1.0
 */
public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        //1.获取连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);






        //2.获取连接
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") // 不写表的时候会监控库下面的所有的表,要写表的话必须要写库名+表名
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> stringSource = env.addSource(sourceFunction);

        stringSource.print();

        env.execute("FlinkCDCWithCustomerDeserialization");


    }
}
