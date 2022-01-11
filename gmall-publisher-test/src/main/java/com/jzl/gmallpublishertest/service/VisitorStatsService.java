package com.jzl.gmallpublishertest.service;

import com.jzl.gmallpublishertest.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:35
 * @Version 1.0
 */

public interface VisitorStatsService {

    //新老访客流量统计

    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    //分时流量统计

    public List<VisitorStats> getVisitorStatsByHour(int date);

    public Long getPv(int date);

    public Long getUv(int date);

}

