package com.jzl.gmallpublishertest.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @Author:JZL
 * @Date: 2022/1/7  17:36
 * @Version 1.0
 */

public interface SugarService {
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);


}
