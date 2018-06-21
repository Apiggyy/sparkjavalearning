package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Persist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\test_spark.txt").cache();

        long startTime = System.currentTimeMillis();
        long count = lines.count();
        long endTime = System.currentTimeMillis();
        System.out.println("costs: " + (endTime - startTime) + "ms...");

        startTime = System.currentTimeMillis();
        count = lines.count();
        endTime = System.currentTimeMillis();
        System.out.println("costs: " + (endTime - startTime) + "ms...");
        sc.close();

    }
}
