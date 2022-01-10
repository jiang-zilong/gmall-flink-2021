package com.jzl.gmallpublishertest.service.impl;

import com.jzl.gmallpublishertest.bean.KeywordStats;
import com.jzl.gmallpublishertest.mapper.KeywordStatsMapper;
import com.jzl.gmallpublishertest.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  15:17
 * @Version 1.0
 */
@Service
public class KeywordStatsServiceImpl  implements KeywordStatsService {
    @Autowired
    private KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date, limit);
    }
}
