package org.sisyphus.demo.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * kafka消费者
 */
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 与kafka连接
        // 指定broker链接地址
        props.setProperty("bootstrap.servers", "hadoop001:9092");
        // key反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 每个消费者都必须属于某一个消费组，所以必须指定group.id
        props.put("group.id", "test");
        // 构造生产者实力对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 拿数据

        // 指定多主题方式1：
        // List<String> topics = new ArrayList<String>();
        // topics.add("mytopic");

        // 指定多主题方式2：
//        List<String> topics = Arrays.asList("mytopic");
        List<String> topics = Arrays.asList("streamingtopic");
        consumer.subscribe(topics);
        try {
            while (true) {
                // timeout 阻塞时间，从kafka中取出100毫秒的数据，有可能一次取出0到N条
                ConsumerRecords<String, String> records = consumer.poll(100);
                // 遍历
                for (ConsumerRecord<String, String> record : records) {
                    // 拿出结果
                    System.out.println("消费者拿到的数据：" + record.value());
                }
            }
        } finally {
            // 关闭
            consumer.close();
        }
    }
}
