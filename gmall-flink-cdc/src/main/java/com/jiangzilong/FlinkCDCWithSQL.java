package com.jiangzilong;

import com.sun.javaws.Main;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author:JZL
 * @Date: 2021/12/15  16:39
 * @Version 1.0
 */
public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

         tableEnv.executeSql("CREATE TABLE mysql_binlog (" +
                " id INT NOT NULL," +
                " tm_name STRING," +
                " logo_url STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'gmall-flink'," +
                " 'table-name' = 'base_trademark'" +
                ")");

        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);


        tuple2DataStream.print();

        env.execute("FlinkCDCWithSQL");

    }
}
