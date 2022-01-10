package com.jzl.gmallpublishertest.mapper;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:08
 * @Version 1.0
 */


import com.jzl.gmallpublishertest.bean.ProvinceStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * Desc:  地区维度统计Mapper
 */

public interface ProvinceStatsMapper {
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats_2021 where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name  ")
    List<ProvinceStats> selectProvinceStats(int date);


}
