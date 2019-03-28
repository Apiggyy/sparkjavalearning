package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class HDFSWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> lines = sc.textFileStream("hdfs://spark1:9000/user/sparklearning/wordcount");
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 9051497322579229973L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairDStream = words.mapToPair(new PairFunction<String,
                String, Integer>() {
            private static final long serialVersionUID = 1873435274918291237L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairDStream.reduceByKey(new Function2<Integer, Integer,
                Integer>() {
            private static final long serialVersionUID = 6191772190711554989L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}
