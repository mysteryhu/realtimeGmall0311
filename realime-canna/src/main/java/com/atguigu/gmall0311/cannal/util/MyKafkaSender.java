package com.atguigu.gmall0311.cannal.util;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Date 2019/8/14
 * @Version JDK 1.8
 **/
public class MyKafkaSender {

    public static KafkaProducer<String,String> kafkaProducer = null;

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;

        producer = new KafkaProducer<String,String>(properties);
        return producer;
    }



    public static void send(String topic, String rowJson) {
        if(kafkaProducer == null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,rowJson));
    }
}
