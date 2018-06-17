package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\test_spark_01.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String,
                Integer>() {
            private static final long serialVersionUID = 4172716118118957678L;

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line, 1);
            }
        });
        JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer,
                Integer>() {
            private static final long serialVersionUID = 5000901778181328371L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 6748116510155173282L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times");
            }
        });
        sc.close();
    }
}
