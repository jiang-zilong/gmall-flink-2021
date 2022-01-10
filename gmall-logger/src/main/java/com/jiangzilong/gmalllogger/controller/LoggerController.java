package com.jiangzilong.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @Author:JZL
 * @Date: 2021/12/14  10:04
 * @Version 1.0
 */
//@Controller
@RestController   //@RestController=@Controller+@RequestBody 表示值返回一个字符串
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    //@ResponseBody
    public String test1() {
        System.out.println("Success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {

        //打印数据
//        System.out.println(jsonStr);

        //将数据落盘
        log.info(jsonStr);
     /* log.warn(jsonStr);
        log.debug(jsonStr);
        log.error(jsonStr);
        log.trace(jsonStr);*/


        //将数据写到kafka
        kafkaTemplate.send("ods_base_log", jsonStr);


        return "success";
    }
}
