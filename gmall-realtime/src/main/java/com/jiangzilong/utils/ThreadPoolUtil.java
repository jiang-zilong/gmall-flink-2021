package com.jiangzilong.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author:JZL
 * @Date: 2022/1/4  15:40
 * @Version 1.0
 *
 * 队列中不会满的话就不会创建新的线程
 *
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    public ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolExecutor.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                            16,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
