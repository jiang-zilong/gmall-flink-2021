package com.jiangzilong.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.jiangzilong.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author:JZL
 * @Date: 2021/12/30  17:20
 * @Version 1.0
 */
public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underCsoreToCamel) throws Exception {

        //创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            //创建泛型对象
            T t = clz.newInstance();
            //给泛型对象赋值  所有的jdbc中都是从1开始进行遍历的
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underCsoreToCamel) {
                    columnName = CaseFormat.UPPER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //获取列值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //将该对象添加至集合
            preparedStatement.close();
            resultSet.close();
            resultList.add(t);
        }
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        List<JSONObject> queryList = queryList(connection,
                "select * from aa ",
                JSONObject.class,
                false);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
