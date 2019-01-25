package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LocalFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("H:/download/test/test02.txt");
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = -7608857128490959501L;

            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });
        int sum = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 625851653000177285L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文字总字数为：" + sum);
    }
}
