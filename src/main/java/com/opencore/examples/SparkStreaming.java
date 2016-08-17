package com.opencore.examples;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkStreaming {

  public static void main(String... args) throws InterruptedException {
    SparkConf conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("Spark Streaming Test Java");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

    processStream(ssc, sc);

    ssc.start();
    ssc.awaitTermination();
  }

  private static void processStream(JavaStreamingContext ssc, JavaSparkContext sc) {
    System.out.println("--> Processing stream");

    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("schema.registry.url", "http://localhost:8081");
    props.put("group.id", "spark");
    props.put("specific.avro.reader", "true");

    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Set<String> topicsSet = new HashSet<>(Collections.singletonList("test"));

    JavaInputDStream<ConsumerRecord<String, Model>> stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, props));

    stream.foreachRDD(rdd -> {
      rdd.foreachPartition(iterator -> {
          while (iterator.hasNext()) {
            ConsumerRecord<String, Model> next = iterator.next();
            System.out.println(next.key() + " --> " + next.value());
          }
        }
      );
    });
  }
}

