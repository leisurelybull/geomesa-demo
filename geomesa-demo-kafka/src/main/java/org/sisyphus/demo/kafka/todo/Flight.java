package org.sisyphus.demo.kafka.todo;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.geotools.data.*;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.visitor.BoundsVisitor;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Flight {
    // reads and parse the command line args
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option kafkaBrokers = OptionBuilder.withArgName("brokers")
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                .create("brokers");
        options.addOption(kafkaBrokers);

        Option zookeepers = OptionBuilder.withArgName("zookeepers")
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka instance, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                .create("zookeepers");
        options.addOption(zookeepers);

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<>();
        dsConf.put("kafka.brokers", cmd.getOptionValue("brokers"));
        dsConf.put("kafka.zookeepers", cmd.getOptionValue("zookeepers"));

        return dsConf;
    }

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
//        // 1.read command line args for a connection to Kafka
//        CommandLineParser parser = new BasicParser();
//        Options options = getCommonRequiredOptions();
//        CommandLine cmd = parser.parse(options, args);

        // 2.create the producer and consumer KafkaDataStore objects
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
        final String sftName = "Airplane";
        final String sftSchema = "flight:String,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);

        System.out.println("Creating schema：" + DataUtilities.encodeType(sft));
        producerDS.createSchema(sft);

        // 4.Loading data
        System.out.println("Generating " + sft.getTypeName() + " data");
        List<SimpleFeature> features = new ArrayList<>();

        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

        builder.set("flight", "G52669");
        builder.set("dtg", Date.from(LocalDateTime.parse("2019-09-03 06:38:11", dateFormat).toInstant(ZoneOffset.UTC)));
        double longitude = Double.parseDouble("98.36544");
        double latitude = Double.parseDouble("43.25563");
        builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

        features.add(builder.buildFeature("G52669"));

        features = Collections.unmodifiableList(features);

        if (true) {
            BoundsVisitor visitor = new BoundsVisitor();
            for (SimpleFeature feature : features) {
                visitor.visit(feature);
            }
            Envelope env = visitor.getBounds();

            System.out.println("Feature type created - register the layer'" + sft.getTypeName() +
                    "' in geoserver with bounds: MinX[" + env.getMinX() + "] MinY[" +
                    env.getMinY() + "] Max[" + env.getMaxX() + "] MaxY[" +
                    env.getMaxY() + "] ");
            System.out.println("Press <enter> to continue");
            System.in.read();
        }

        writeFeatures(dataStore, sft, features);
        SimpleFeatureStore producerFS = (SimpleFeatureStore) dataStore.getFeatureSource(sft.getTypeName());
        SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sft.getTypeName());
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");

        int n = 0;
        for (SimpleFeature feature : features) {
            producerFS.addFeatures(new ListFeatureCollection(sft, Collections.singletonList(feature)));
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                return;
            }
            System.out.println("Current consumer state:");
            System.out.println(DataUtilities.encodeFeature(feature));
        }

        System.out.println();
        System.out.println("Done");
        System.exit(0);
    }
}
