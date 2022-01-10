package com.jzl.gmallpublishertest.bean;

/**
 * @Author:JZL
 * @Date: 2022/1/10  14:04
 * @Version 1.0
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Desc: 地区交易额统计实体类
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    private BigDecimal order_amount;
    private String ts;
}
