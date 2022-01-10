package com.jiangzilong.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jiangzilong.bean.TableProcess;
import com.jiangzilong.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


/**
 * @Author:JZL
 * @Date: 2021/12/21  10:06
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);

    }

    // value:{"db":"","tablename":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取数据并解析
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    //建表语句:    create table if not exists db.tn(id varchar primary key,tm_name varchar)xxx
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer(" create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                //判断是否是主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }

                //判断是否是最后一个字段  不是 则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);

            //打印检表数据
            System.out.println(createTableSQL);


            //预编译sql
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("phoenix表" + sinkTable + "建表失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String key = value.getString("tableName" + "-" + value.getString("type"));
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //将输出表/主题信息写入到value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();

            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //kafka数据 写入到主流
                out.collect(value);
            }
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //hbase数据，写入到侧输出流
                ctx.output(objectOutputTag, value);
            }


        } else {
            System.out.println("该组合key：" + key + "不存在");
        }


    }

    /**
     * @param data                           {"id":"","name":"","url":""}
     * @param sinkColumns{"id":"","name":""}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        /*        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columns.contains(next.getKey())) {
                iterator.remove();
            }
        }*/
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }

}
