package com.jzl.gmallpublishertest.service.impl;

import com.jzl.gmallpublishertest.mapper.ProductStatsMapper;
import com.jzl.gmallpublishertest.service.SugarService;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author:JZL
 * @Date: 2022/1/7  17:37
 * @Version 1.0
 */
public class SugarServiceImpl implements SugarService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }



    @Override
    public Map getGmvByTm(int date, int limit) {
        //查询数据
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        //创建map用于存放数据
        HashMap<String, BigDecimal> result = new HashMap<>();

        //遍历mapList 将数据取出放入result  Map[("tm_name"->"苹果"),(order_amount->279501)]
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        //返回结果
        return result;
    }
}
