package com.jzl.app;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author:JZL
 * @Date: 2021/12/30  14:57
 * @Version 1.0
 */
public class test {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        long time = sdf.parse("2021-12-12 15:50:50").getTime();
        System.out.println(time); //1639295450000

    }
}
