package com.self.relearning.core;

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

    public static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        JavaRDD<Integer> multiNumbersRDD = numbersRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = -2153037644971776628L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        multiNumbersRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -1766920897629263719L;

            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });

        sc.close();
    }

    public static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        JavaRDD<Integer> evenNumbersRDD = numbersRDD.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = 6512219550008893213L;

            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        evenNumbersRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 2248050771335997453L;

            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });

        sc.close();
    }

    public static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> lines = sc.parallelize(lineList);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 879471289452933108L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        words.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 6648768274683465036L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }

    public static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<>("Class1", 80),
                new Tuple2<>("Class2", 75),
                new Tuple2<>("Class2", 98),
                new Tuple2<>("Class1", 91)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 5829600945486980554L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                for (Integer score : t._2) {
                    System.out.println(score);
                }
                System.out.println("=================================");
            }
        });
        sc.close();
    }

    public static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<>("Class1", 80),
                new Tuple2<>("Class2", 75),
                new Tuple2<>("Class2", 98),
                new Tuple2<>("Class1", 91)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -9024810861703647736L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 3553693401358839450L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " : " + t._2);
            }
        });
    }

    public static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<>(65, "tom"),
                new Tuple2<>(85, "leo"),
                new Tuple2<>(76, "Alice")
        );
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, String> sortedScores = scores.sortByKey(false);
        sortedScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = -6068865152964360569L;

            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + " : " + t._2);
            }
        });
        sc.close();
    }

    public static void join() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> studentList = Arrays.asList(
                new Tuple2<>(1,"leo"),
                new Tuple2<>(2,"jack"),
                new Tuple2<>(3,"tom")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 60)
        );
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Tuple2<String, Integer>> info = students.join(scores);
        info.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            private static final long serialVersionUID = 1119721371814504408L;

            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println(t._1 + "\t" + t._2._1 + "\t" + t._2._2);
            }
        });

        sc.close();
    }

    public static void cogroup() {
        SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> studentList = Arrays.asList(
                new Tuple2<>(1,"leo"),
                new Tuple2<>(2,"jack"),
                new Tuple2<>(3,"tom")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(1, 70),
                new Tuple2<>(2, 61),
                new Tuple2<>(3, 60),
                new Tuple2<>(1, 45),
                new Tuple2<>(3, 88)
        );
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> info = students.cogroup(scores);
        info.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            private static final long serialVersionUID = 3195669190694656640L;

            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws
                    Exception {
                System.out.println(t._1);
                System.out.println(t._2._1);
                System.out.println(t._2._2);
                System.out.println("===========================================");
            }
        });

    }



}
