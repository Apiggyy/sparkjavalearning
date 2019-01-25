package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> numberRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        Integer totalNum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -8607399335788449660L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("totalNum : " + totalNum);
        sc.close();
    }
}
