package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SortedWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortedWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("H:/download/test/test02.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -2062209018409248076L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 174883022509535908L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -1695636364553711933L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> countWord = wordCount.mapToPair(new PairFunction<Tuple2<String,
                Integer>, Integer, String>() {
            private static final long serialVersionUID = -8937164944935958525L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });
        JavaPairRDD<Integer, String> sortedCountWord = countWord.sortByKey(false);
        JavaPairRDD<String, Integer> sortedWordCount = sortedCountWord.mapToPair(new PairFunction<Tuple2<Integer, String>,
                String, Integer>() {
            private static final long serialVersionUID = 2856730879767302843L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        sortedWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 7190485799100151694L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times...");
            }
        });

        sc.close();
    }
}
