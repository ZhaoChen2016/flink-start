package com.jd.flink.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class ConfigUtil {
    public static String kafkaBroker;
    public static String kafkaSourceTopic;
    public static String kafkaSinkTopic;
    public static String kafkaConsumerGroup;
    public static String kafkaClientId;
    public static Properties loadProperties(String configPath){
        log.info("start reading config file : "+ configPath);
        Properties properties = new Properties();
        InputStream kafkaInStream = CommonUtil.class.getClassLoader().getResourceAsStream(configPath);
        try {
            properties.load(kafkaInStream);
            kafkaBroker = properties.getProperty("kafka.broker");
            kafkaSourceTopic = properties.getProperty("kafka.source.topic");
            kafkaSinkTopic = properties.getProperty("kafka.sink.topic");
            kafkaConsumerGroup = properties.getProperty("kafka.consumer.group");
            kafkaClientId = properties.getProperty("kafka.client.id");
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("finish reading config file :{}",configPath);
        return properties;
    }

}
