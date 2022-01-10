package com.jzl.gmallpublishertest.service.impl;

import com.jzl.gmallpublishertest.bean.ProvinceStats;
import com.jzl.gmallpublishertest.mapper.ProductStatsMapper;
import com.jzl.gmallpublishertest.mapper.ProvinceStatsMapper;
import com.jzl.gmallpublishertest.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:16
 * @Version 1.0
 */

/**
 * Desc:    按地区维度统计Service实现
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    private ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
