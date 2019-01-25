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

public class WordCountCluster {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountCluster");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/user/test02.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 6830067812565336294L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 4129508765313427257L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1116331106618618827L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 5202191961979094816L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times.");
            }
        });
    }
}
