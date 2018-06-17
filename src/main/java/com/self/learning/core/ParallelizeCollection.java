package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD = sc.parallelize(numbers);
        Integer sum = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -7128191353163575082L;
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });
        System.out.println("sum : " + sum);
        sc.close();
    }

}
