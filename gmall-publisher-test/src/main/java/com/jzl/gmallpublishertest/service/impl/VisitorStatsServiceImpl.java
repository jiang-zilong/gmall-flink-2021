package com.jzl.gmallpublishertest.service.impl;

import com.jzl.gmallpublishertest.bean.VisitorStats;
import com.jzl.gmallpublishertest.mapper.VisitorStatsMapper;
import com.jzl.gmallpublishertest.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:55
 * @Version 1.0
 */
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(int date) {
        return visitorStatsMapper.selectPv(date);
    }

    @Override
    public Long getUv(int date) {
        return visitorStatsMapper.selectUv(date);
    }

}
