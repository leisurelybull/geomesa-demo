package org.sisyphus.demo.kafka.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka生产者
 */
public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 与kafka连接
        props.setProperty("bootstrap.servers", "hadoop001:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 发送
        for(int i = 0 ; i < 100 ; i++){
            System.out.println(i);
            producer.send(new ProducerRecord<String, String>("mytopic", "hello"+i));
        }

        // 关闭
        producer.close();
    }
}
