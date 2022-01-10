package com.jiangzilong;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;

/**
 * @Author:JZL
 * @Date: 2021/12/15  10:45
 * @Version 1.0
 */
public class FlinkCDCSinkStraRocks {
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
                .tableList("gmall-flink.base_trademark")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> stringSource = env.addSource(sourceFunction);

        stringSource.print();


        stringSource.addSink(StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://192.168.11.110:9030")
                        .withProperty("load-url", "192.168.11.110:8030")
                        .withProperty("username", "root")
                        .withProperty("password", "dreamtech")
                        .withProperty("table-name", "base_trademark")
                        .withProperty("database-name", "flink_cdc_starrocks")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .withProperty("sink.buffer-flush.interval-ms","1000")
                        .build()
        ));

        env.execute("FlinkCDCSinkStraRocks");


    }

}
