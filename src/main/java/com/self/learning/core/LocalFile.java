package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LocalFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\test_spark.txt");
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = -4294166382487867787L;

            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });
        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -5437722908076797183L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总字数是：" + count);

        sc.close();
    }
}
