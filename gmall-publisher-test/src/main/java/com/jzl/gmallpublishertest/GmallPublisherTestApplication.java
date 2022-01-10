package com.jzl.gmallpublishertest;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.jzl.gmallpublishertest.mapper")
public class GmallPublisherTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherTestApplication.class, args);
    }

}
