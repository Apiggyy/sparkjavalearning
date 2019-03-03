package com.self.relearning.core;

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
        //saveAsTextFile();
        countByKey();
    }

    public static void reduce() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        Integer total = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 8336973802530866332L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(total);
        sc.close();
    }

    public static void collect() {
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        JavaRDD<Integer> multiNumbers = numbers.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 8501181154516185842L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        List<Integer> colNumbers = multiNumbers.collect();
        for (Integer number : colNumbers) {
            System.out.println(number);
        }

        sc.close();
    }

    public static void saveAsTextFile() {
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        JavaRDD<Integer> multiNumbers = numbers.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 8501181154516185842L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        multiNumbers.saveAsTextFile("H:/download/test/test03.txt");
    }

    public static void countByKey() {
        SparkConf conf = new SparkConf().setAppName("countByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> studentsList = Arrays.asList(
                new Tuple2<>("class1", "leo"),
                new Tuple2<>("class2", "jack"),
                new Tuple2<>("class1", "marry"),
                new Tuple2<>("class2", "tom"),
                new Tuple2<>("class2", "david")
        );
        JavaPairRDD<String, String> students = sc.parallelizePairs(studentsList);
        Map<String, Long> studentsCount = students.countByKey();
        System.out.println(studentsCount);

        sc.close();
    }


}
