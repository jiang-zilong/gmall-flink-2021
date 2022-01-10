package com.jzl.gmallpublishertest.mapper;

import com.jzl.gmallpublishertest.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author:JZL
 * @Date: 2022/1/10  15:11
 * @Version 1.0
 */
public interface KeywordStatsMapper {
    @Select("select keyword," +
            "sum(keyword_stats_2021.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
            " from keyword_stats_2021 where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(keyword_stats_2021.ct) desc limit #{limit} ")
    List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}
