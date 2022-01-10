package com.jiangzilong.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @Author:JZL
 * @Date: 2022/1/4  17:01
 * @Version 1.0
 */
public interface DimAsyncJoinFunction<T> {

     String getKey(T input);

     void join(T input, JSONObject dimInfo) throws ParseException;


}
