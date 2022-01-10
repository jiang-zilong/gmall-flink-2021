package com.jiangzilong.app.function;

import com.alibaba.fastjson.JSONObject;
import com.jiangzilong.common.GmallConfig;
import com.jiangzilong.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @Author:JZL
 * @Date: 2021/12/22  14:59
 * @Version 1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取sql
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");

            String upsertsql = getUpsertSql(sinkTable,
                    after);
            System.out.println(upsertsql);

            //预编译
            preparedStatement = connection.prepareStatement(upsertsql);

            //判断当前数据是update操作，先删除redis中数据
            if ("update".equals(value.getString("type"))) {
            DimUtil.delRedisDimInfi(sinkTable.toUpperCase(), after.getString("id"));
            }


            //执行插入操作
            preparedStatement.executeUpdate();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }

        }

    }

    private String getUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keyset = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into" + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keyset, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";

    }
}

