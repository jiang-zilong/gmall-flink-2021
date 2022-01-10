package com.jiangzilong.utils;

import com.alibaba.fastjson.JSONObject;
import com.jiangzilong.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author:JZL
 * @Date: 2021/12/30  18:00
 * @Version 1.0
 * 在封装一下 方便使用
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        //查询之前先查询redis

        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {

            //重置过期时间为24小时
            jedis.expire(redisKey, 20 * 60 * 60);

            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }


        //拼接查询语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + "where id='" + id + "'";

        //查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);
        //在返回结果之前写入到redis'
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfi(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    /**
     * 优化的必要性：测试 每秒读取数据只有80条，10并发只有800条，高峰期超过  就会产生反压
     */
    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();

        System.out.println(getDimInfo(connection, "DIM_USER_INfo", "143"));

        long end = System.currentTimeMillis();

        System.out.println(end - start);

        connection.close();
    }

}
