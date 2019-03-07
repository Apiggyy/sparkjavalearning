package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupTop3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("H:/download/test/score.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -4345010847963768799L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> groupParis = pairs.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> sortedGroupPairs = groupParis.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            private static final long serialVersionUID = -1254418121729264224L;

            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
                    throws Exception {
                Integer[] top3Scores = new Integer[3];
                for (Integer score : t._2) {
                    for (int i = 0; i < top3Scores.length; i++) {
                        if (top3Scores[i] == null) {
                            top3Scores[i] = score;
                            break;
                        } else if (top3Scores[i] < score) {
                            for (int j = 2; j > i; j--) {
                                top3Scores[j] = top3Scores[j - 1];
                            }
                            top3Scores[i] = score;
                            break;
                        }
                    }
                }
                return new Tuple2<>(t._1, Arrays.asList(top3Scores));
            }
        });
        sortedGroupPairs.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 149044219591457568L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                System.out.println(t._2);
                System.out.println("===================================");
            }
        });
        sc.close();
    }
}
