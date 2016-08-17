package com.opencore.examples;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class SparkStreaming {

  public static void main(String... args) {
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

    Map<String, String> props = new HashMap<>();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("schema.registry.url", "http://localhost:8081");
    props.put("group.id", "spark");
    props.put("specific.avro.reader", "true");

    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Set<String> topicsSet = new HashSet<>(Collections.singletonList("test"));

    JavaPairInputDStream<String, Object> stream = KafkaUtils.createDirectStream(ssc, String.class, Object.class,
      StringDecoder.class, KafkaAvroDecoder.class, props, topicsSet);

    stream.foreachRDD(rdd -> {
      rdd.foreachPartition(iterator -> {
          while (iterator.hasNext()) {
            Tuple2<String, Object> next = iterator.next();
            Model model = (Model) next._2();
            System.out.println(next._1() + " --> " + model);
          }
        }
      );
    });
  }
}

