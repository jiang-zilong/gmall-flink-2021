package com.jzl.gmallpublishertest.service;

import com.jzl.gmallpublishertest.bean.ProvinceStats;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:15
 * @Version 1.0
 */

public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
