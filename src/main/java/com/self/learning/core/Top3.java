package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> numbers = sc.textFile("E:\\迅雷下载\\top.txt");
        JavaPairRDD<Integer, String> pairs = numbers.mapToPair(new PairFunction<String, Integer,
                String>() {
            private static final long serialVersionUID = 378891992171046790L;

            @Override
            public Tuple2<Integer, String> call(String t) throws Exception {
                return new Tuple2<>(Integer.valueOf(t), t);
            }
        });
        JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);
        JavaRDD<Integer> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            private static final long serialVersionUID = -2727689133241636421L;

            @Override
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });
        List<Integer> top3 = sortedNumbers.take(3);
        for (Integer num : top3) {
            System.out.println(num);
        }
        sc.close();
    }
}
