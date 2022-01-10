package com.jiangzilong.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Author:JZL
 * @Date: 2022/1/4  19:04
 * @Version 1.0
 */
public class DateTimeUtil {
    private final static DateTimeFormatter formater =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),
                ZoneId.systemDefault());
        return formater.format(localDateTime);
    }
    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formater);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();

    }
}
