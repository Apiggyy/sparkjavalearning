package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionOperation {

    public static void main(String[] args) {
        //reduce();
        //collect();
        //count();
        //take();
        //saveAsTextFile();
        countByKey();
    }

    private static void reduce() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        Integer sum = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -1216798571311646994L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("总和为: " + sum);
        sc.close();
    }

    private static void collect() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        JavaRDD<Integer> multiNumbersRDD = numbersRDD.map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = -9201781056451731358L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        List<Integer> multiNumbers = multiNumbersRDD.collect();
        for (Integer multiNumber : multiNumbers) {
            System.out.println(multiNumber);
        }
        sc.close();
    }

    private static void count() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        long count = numbersRDD.count();
        System.out.println(count);
        sc.close();
    }

    private static void take() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        List<Integer> top3Number = numbersRDD.take(3);
        for (Integer number : top3Number) {
            System.out.println(number);
        }
        sc.close();
    }

    private static void saveAsTextFile() {
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        JavaRDD<Integer> doubleNumberRDD = numbersRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 814941147417847119L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        doubleNumberRDD.saveAsTextFile("hdfs://spark1:9000/user/data/doubleNumber.txt");
        sc.close();
    }

    private static void countByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> studentList = Arrays.asList(
                new Tuple2<>("class1", "Alice"),
                new Tuple2<>("class2", "Bob"),
                new Tuple2<>("class1", "Cindy"),
                new Tuple2<>("class2", "David"),
                new Tuple2<>("class1", "Javk"));

        JavaPairRDD<String, String> studentsRDD = sc.parallelizePairs(studentList);
        Map<String, Long> map = studentsRDD.countByKey();
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }




}
