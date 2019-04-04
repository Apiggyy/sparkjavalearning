package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaReceiverWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put("WordCount", 1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(sc, "spark1:2181," +
                "spark2:2181,spark3:2181", "spark_ test", topicThreadMap);
        JavaDStream<String> words = lines.flatMap(t -> Arrays.asList(t._2.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDStream = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairDStream.reduceByKey((v1, v2) -> v1 + v2);
        wordCounts.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}
