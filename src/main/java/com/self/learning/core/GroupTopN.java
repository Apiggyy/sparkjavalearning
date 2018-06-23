package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class GroupTopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GrouptopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\groupTop.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -8918367110372019983L;
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<>(t.split(" ")[0], Integer.valueOf(t.split(" ")[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> classScores = pairs.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> classScoresTopN = classScores.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            private static final long serialVersionUID = 8794142530474291194L;

            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
                    throws Exception {
                int n = 3;
                Integer[] topN = new Integer[n];
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    Integer score = iterator.next();
                    for (int i = 0; i < n; i++) {
                        if (topN[i] == null) {
                            topN[i] = score;
                            break;
                        } else {
                            //(5,4,3,1) 6 => (5,4,3,3) => (5,4,4,3) => (5,5,4,3) => (6,5,4,3)
                            //                j=0		  j=1			j=2
                            //
                            //(8,5,4,1) 6 => (8,5,4,4) => (8,5,5,4) => (8,6,5,4)
                            //                j=0          j=1
                            if (topN[i] < score) {
                                int moveTimes = n - i - 1;
                                for (int j = 0; j < moveTimes; j++) {
                                    topN[n - 1 - j] = topN[n - 1 - j - 1];
                                }
                                topN[i] = score;
                                break;
                            }
                        }
                    }
                }
                return new Tuple2<>(t._1, Arrays.asList(topN));
            }
        });

        classScoresTopN.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 3577744410874977859L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                System.out.println("-----------------------------");
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println();
            }
        });

    }
}
