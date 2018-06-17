package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class HdfsFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HdfsFile");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/user/data/test_spark.txt");
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = -9216104884542425703L;

            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });
        Integer count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -5159917128006407330L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总字数为：" + count);
        sc.close();
    }
}
