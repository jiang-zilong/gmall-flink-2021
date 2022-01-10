package com.jzl.gmallpublishertest.service;

import com.jzl.gmallpublishertest.bean.KeywordStats;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  15:15
 * @Version 1.0
 */
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(int date, int limit);
}
