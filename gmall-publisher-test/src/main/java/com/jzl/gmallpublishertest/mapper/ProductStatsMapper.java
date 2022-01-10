package com.jzl.gmallpublishertest.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @Author:JZL
 * @Date: 2022/1/7  17:37
 * @Version 1.0
 */

public interface ProductStatsMapper {

    //GMV
    @Select("select sum(order_amount) from product_stats_210325 where toYYYYMMDD(stt)=#{date}")
    //返回值是bigDecimal类型的数据
    BigDecimal selectGmv(int date);



    /**
     * ┌─tm_name─┬─order_amount─┐
     * │ 苹果    │    279501.00 │
     * │ 华为    │    206697.00 │
     * │ TCL     │    182378.00 │
     * │ 小米    │    100278.00 │
     * │ Redmi   │     31872.00 │
     * └─────────┴──────────────┘
     *
     * Map[("tm_name"->"苹果"),(order_amount->279501)]  ==> [("苹果"->279501),...]
     */
    //统计某天不同SPU商品交易额排名
    @Select("select tm_name,sum(order_amount) order_amount from product_stats_210325 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_name order by order_amount desc limit #{limit}")
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);






}
