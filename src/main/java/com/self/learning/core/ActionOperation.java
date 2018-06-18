package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ActionOperation {

    public static void main(String[] args) {
        reduce();
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
}
