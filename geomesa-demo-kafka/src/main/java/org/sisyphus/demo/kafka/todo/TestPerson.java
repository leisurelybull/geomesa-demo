package org.sisyphus.demo.kafka.todo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.geotools.data.*;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TestPerson {
    public static void writeFeatures(DataStore dataStore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            System.out.println("Writing " + sft.getTypeName() + " date");
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                         dataStore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    SimpleFeature toWrite = writer.next();
                    toWrite.setAttributes(feature.getAttributes());
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                    toWrite.getUserData().putAll(feature.getUserData());

                    writer.write();
                }
            }
            System.out.println("Wrote " + features.size() + " features");
            System.out.println();
        }
    }

    public static void main(String[] args) throws Exception {


        Map<String, String> dsConf = new HashMap<>();
        dsConf.put("kafka.brokers", "hadoop001:9092");
        dsConf.put("kafka.zookeepers", "hadoop001:2181");
//        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
        System.out.println("Loading datastore");
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);

        dsConf.put("kafka.consumer.count", "0");
        DataStore producerDS = DataStoreFinder.getDataStore(dsConf);
        dsConf.put("kafka.consumer.count", "1");
        DataStore consumerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore objects properly
        if (producerDS == null) {
            throw new Exception("Null producer KafkaDataStore");
        }
        if (consumerDS == null) {
            throw new Exception("Null consumer KafkaDataStore");
        }

        // 3.create the schema which creates a topic in Kafka
        // only needs to be done once
        final String sftName = "TestPerson";
        final String sftSchema = "name:String,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);

        System.out.println("Creating schema：" + DataUtilities.encodeType(sft));
        producerDS.createSchema(sft);

        // 4.Loading data
        System.out.println("Generating " + sft.getTypeName() + " data");
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
        // 构造消费者实例对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 拿数据
        List<String> topics = Arrays.asList("streamingtopic");
        consumer.subscribe(topics);

        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        while (true) {
//             timeout 阻塞时间，从kafka中取出100毫秒的数据，有可能一次取出0到N条
            ConsumerRecords<String, String> records = consumer.poll(100);
//             遍历
            for (ConsumerRecord<String, String> record : records) {
                List<SimpleFeature> features = new ArrayList<>();
                // 拿出结果
                System.out.println(record.value());

                String[] a = record.value().split("\t");

                builder.set("name", a[0]);
                double longitude = Double.parseDouble(a[1]);
                double latitude = Double.parseDouble(a[2]);
                builder.set("geom", "POINT (" + longitude + " " + latitude + ")");
                builder.set("dtg", Date.from(LocalDateTime.parse(a[3], dateFormat).toInstant(ZoneOffset.UTC)));

                builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                features.add(builder.buildFeature(a[0]));

                features = Collections.unmodifiableList(features);
//                System.out.println(features);

                writeFeatures(dataStore, sft, features);
                SimpleFeatureStore producerFS = (SimpleFeatureStore) dataStore.getFeatureSource(sft.getTypeName());
//            SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sft.getTypeName());
//            System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");

                int n = 0;
                for (SimpleFeature feature : features) {
                    producerFS.addFeatures(new ListFeatureCollection(sft, Collections.singletonList(feature)));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    System.out.println("Current consumer state:");
                    System.out.println(DataUtilities.encodeFeature(feature));
                }
            }
        }
    }
}
