package com.jzl.gmallpublishertest.controller;

import com.alibaba.fastjson.JSON;
import com.jzl.gmallpublishertest.bean.KeywordStats;
import com.jzl.gmallpublishertest.bean.ProvinceStats;
import com.jzl.gmallpublishertest.bean.VisitorStats;
import com.jzl.gmallpublishertest.mapper.KeywordStatsMapper;
import com.jzl.gmallpublishertest.service.KeywordStatsService;
import com.jzl.gmallpublishertest.service.ProvinceStatsService;
import com.jzl.gmallpublishertest.service.SugarService;
import com.jzl.gmallpublishertest.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author:JZL
 * @Date: 2022/1/7  17:36
 * @Version 1.0
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private SugarService sugarService;

    @Autowired
    private ProvinceStatsService provinceStatsService;

    @Autowired
    private VisitorStatsService visitorStatsService;

    @Autowired
    private KeywordStatsService keywordStatsService;

    @RequestMapping("/keyword")
    public String getKeywordStats(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit",defaultValue = "20") int limit){
        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        //查询数据
        List<KeywordStats> keywordStatsList
                = keywordStatsService.getKeywordStats(date, limit);
        StringBuilder jsonSb=new StringBuilder( "{\"status\":0,\"msg\":\"\",\"data\":[" );
        //循环拼接字符串
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats =  keywordStatsList.get(i);
            if(i>=1){
                jsonSb.append(",");
            }
            jsonSb.append(  "{\"name\":\"" + keywordStats.getKeyword() + "\"," +
                    "\"value\":"+keywordStats.getCt()+"}");
        }
        jsonSb.append(  "]}");
        return  jsonSb.toString();
    }







    @RequestMapping("/hr")
    public String getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date ) {
        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        List<VisitorStats> visitorStatsHrList
                = visitorStatsService.getVisitorStatsByHour(date);

        //构建24位数组
        VisitorStats[] visitorStatsArr=new VisitorStats[24];

        //把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorStatsHrList) {
            visitorStatsArr[visitorStats.getHr()] =visitorStats ;
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();

        //循环出固定的0-23个小时  从结果map中查询对应的值
        for (int hr = 0; hr <=23 ; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats!=null){
                uvList.add(visitorStats.getUv_ct())   ;
                pvList.add( visitorStats.getPv_ct());
                newMidList.add( visitorStats.getNew_uv());
            }else{ //该小时没有流量补零
                uvList.add(0L)   ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            //小时数不足两位补零
            hrList.add(String.format("%02d", hr));
        }
        //拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return  json;
    }

    @RequestMapping("/visitor")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        List<VisitorStats> visitorStatsByNewFlag = visitorStatsService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        //循环把数据赋给新访客统计对象和老访客统计对象
        for (VisitorStats visitorStats : visitorStatsByNewFlag) {
            if (visitorStats.getIs_new().equals("1")) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        //把数据拼接入字符串
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新用户\",\"id\":\"new\"}," +
                "{\"name\":\"老用户\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"用户数(人)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"总访问页面(次)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"跳出率(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"平均访问页面数(人次)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return json;
    }


    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        StringBuilder jsonBuilder = new StringBuilder("{\"status\":0,\"data\":{\"mapData\":[");
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        if (provinceStatsList.size() == 0) {
            //    jsonBuilder.append(  "{\"name\":\"北京\",\"value\":0.00}");
        }
        for (int i = 0; i < provinceStatsList.size(); i++) {
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            ProvinceStats provinceStats = provinceStatsList.get(i);
            jsonBuilder.append("{\"name\":\"" + provinceStats.getProvince_name() + "\",\"value\":" + provinceStats.getOrder_amount() + " }");

        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();

    }


    @RequestMapping("/tm")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        Map gmvByTm = sugarService.getGmvByTm(date, limit);
        Set keySet = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(keySet, "\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"商品品牌\", " +
                "        \"data\": [" +
                StringUtils.join(values, ",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }


    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            //没传日期默认值就是当天的时间
            date = getToday();
        }
        HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", 0);
        result.put("data", sugarService.getGmv(date));

        return JSON.toJSONString(result);

//        return "        { " +
////                "          \"status\": 0, " +
////                "          \"msg\": \"\", " +
////                "          \"data\": " + sugarService.getGmv(date) + " " +
////                "        }";
    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateTime = sdf.format(System.currentTimeMillis());
        return Integer.parseInt(dateTime);
    }


}
