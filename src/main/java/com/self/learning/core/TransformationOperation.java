package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {

    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        //join();
        cogroup();
    }

    private static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> newJavaRDD = javaRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 7208830674410898548L;

            @Override
            public Integer call(Integer val) throws Exception {
                return val * 2;
            }
        });
        newJavaRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -4036094346976687349L;

            @Override
            public void call(Integer val) throws Exception {
                System.out.println(val);
            }
        });
        sc.close();
    }

    private static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> newJavaRDD = javaRDD.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = 464641919900154279L;

            @Override
            public Boolean call(Integer val) throws Exception {
                return val % 2 == 0;
            }
        });
        newJavaRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 7686509254326950345L;

            @Override
            public void call(Integer val) throws Exception {
                System.out.println(val);
            }
        });
        sc.close();
    }

    private static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> lines = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> javaRDD = sc.parallelize(lines);
        JavaRDD<String> newJavaRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 6496826094411891900L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        newJavaRDD.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -8106739777363218233L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }

    private static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 90),
                new Tuple2<>("class1", 70),
                new Tuple2<>("class2", 60));
        JavaPairRDD<String, Integer> scorePairs = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> score = scorePairs.groupByKey();
        score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = -8397329664863945719L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("=====================================");
            }
        });
        sc.close();
    }

    private static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 80),
                new Tuple2<>("class1", 70),
                new Tuple2<>("class2", 60));

        JavaPairRDD<String, Integer> scoreList = sc.parallelizePairs(list);

        JavaPairRDD<String, Integer> scores = scoreList.reduceByKey(new Function2<Integer, Integer,
                Integer>() {
            private static final long serialVersionUID = 5187172695735383440L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        scores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 266773633007451584L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("class: " + t._1);
                System.out.println("score: " + t._2);
                System.out.println("======================================");
            }
        });
        sc.close();
    }

    private static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<>(80, "Alice"),
                new Tuple2<>(70, "Bob"),
                new Tuple2<>(100, "Cindy"),
                new Tuple2<>(90, "Jack"));

        JavaPairRDD<Integer, String> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, String> scoreSorted = scoreRDD.sortByKey(false);

        scoreSorted.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = 5089437125890666611L;

            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + " : " + t._2);
            }
        });
        sc.close();
    }

    private static void join() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "Alice"),
                new Tuple2<>(2, "Tom"),
                new Tuple2<>(3, "Jack"));

        List<Tuple2<Integer,Integer>> scores = Arrays.asList(
                new Tuple2<>(1,90),
                new Tuple2<>(2,80),
                new Tuple2<>(3,60));

        JavaPairRDD<Integer, String> studentsRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentsRDD.join(scoreRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            private static final long serialVersionUID = -5711390963001761981L;

            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("id : " + t._1);
                System.out.println("name : " + t._2._1);
                System.out.println("score : " + t._2._2);
                System.out.println("===================================");
            }
        });

        sc.close();

    }

    private static void cogroup() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "Alice"),
                new Tuple2<>(2, "Tom"),
                new Tuple2<>(3, "Jack"));

        List<Tuple2<Integer,Integer>> scores = Arrays.asList(
                new Tuple2<>(1,90),
                new Tuple2<>(2,80),
                new Tuple2<>(3,60),
                new Tuple2<>(1,60),
                new Tuple2<>(2,100));

        JavaPairRDD<Integer, String> studentsRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = studentsRDD.cogroup(scoreRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            private static final long serialVersionUID = 363119086165788322L;

            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws
                    Exception {
                System.out.println("id : " + t._1);
                System.out.println("id : " + t._2._1);
                System.out.println("id : " + t._2._2);
                System.out.println("===========================================");
            }
        });

        sc.close();

    }

}
