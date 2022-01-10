package com.jzl;

import com.google.gson.Gson;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author:JZL
 * @Date: 2021/12/22  9:30
 * @Version 1.0
 */
public class FlinkToStarRocksTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FlinkToStarRocksTest ft = new FlinkToStarRocksTest();
        ft.useStream(env);
        env.execute();
    }

    void useStream(StreamExecutionEnvironment env) {
        Gson gson = new Gson();
        DataStreamSource<String> studentDataStreamSource = env.fromCollection(Arrays.asList(gson.toJson(new Student(1001, "张三", 18, "111@qq.com", "男")),
                gson.toJson(new Student(1002, "李四", 19, "222@qq.com", "女")),
                gson.toJson(new Student(1003, "王五", 20, "333@qq.com", "男"))));



        studentDataStreamSource.print();



        studentDataStreamSource.addSink(StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://192.168.11.110:9030")
                        .withProperty("load-url", "192.168.11.110:8030")
                        .withProperty("username", "root")
                        .withProperty("password", "dreamtech")
                        .withProperty("table-name", "flink_student")
                        .withProperty("database-name", "flink_cdc_starrocks")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        .build()
                )
        );
    }

}

class Student {
    private int uid;
    private String name;
    private int age;
    private String email;
    private String sex;

    public Student(int uid, String name, int age, String email, String sex) {
        this.uid = uid;
        this.name = name;
        this.age = age;
        this.email = email;
        this.sex = sex;
    }
}
