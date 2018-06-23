package com.self.learning.core;

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

public class SortWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\test_spark.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 2371710213445603872L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String,
                Integer>() {
            private static final long serialVersionUID = -7789848228114008836L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer,
                Integer>() {
            private static final long serialVersionUID = -599651113325434191L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> countWords = wordsCount.mapToPair(new PairFunction<Tuple2<String,
                Integer>, Integer, String>() {
            private static final long serialVersionUID = -3776631838756804780L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });
        JavaPairRDD<Integer, String> sortCountWords = countWords.sortByKey(false);

        wordsCount = sortCountWords.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            private static final long serialVersionUID = 1850423128750624029L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });
        wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -8206978419200606710L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times.");
            }
        });
        sc.close();
    }
}
