package com.jiangzilong.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author:JZL
 * @Date: 2021/12/16  14:39
 * @Version 1.0
 */
public class MyKafkaUtil {

    private static String brokers = "hadoop102:9092,hadoop103:9092,hadoop103:9092";
    private static String defaultTopic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());

    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaProducer<T>(defaultTopic,
                kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.NONE);
    }


    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String gropuId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, gropuId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return  " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }

}
