package com.self.relearning.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "spark1:9092,spark2:9092,spark3:9092");
        Set<String> topics = new HashSet<>();
        topics.add("WordCount");
        JavaPairInputDStream<String, String> lines
                = KafkaUtils.createDirectStream(sc, String.class, String.class
                , StringDecoder.class, StringDecoder.class,
                kafkaParams, topics);
        JavaDStream<String> words = lines.flatMap(t -> Arrays.asList(t._2.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDStream = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairDStream.reduceByKey((v1, v2) -> v1 + v2);
        wordCounts.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}
