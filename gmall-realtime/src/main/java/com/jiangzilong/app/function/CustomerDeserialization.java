package com.jiangzilong.app.function;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2021/12/16  9:31
 * @Version 1.0
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {


    /**
     * 封装的数据格式：json格式
     * {
     * "库":"",
     * "表":"",
     * "before":"{"":"","":""}",
     * "after":"{"":"","":""}"
     * "操作类型":"c  u d",
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建json对象用于存储最终数据
        JSONObject result = new JSONObject();
        //2.获取库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();
        //3.获取before数据

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();

        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();

            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        //4.获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        //5.获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //6.将字段写入json
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //7.写出
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
