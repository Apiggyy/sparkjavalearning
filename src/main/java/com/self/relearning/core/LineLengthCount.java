package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class LineLengthCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineLengthCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("H:/download/test/test01.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -8735765165583269577L;

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line, 1);
            }
        });
        JavaPairRDD<String, Integer> lineLength = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1873620423343235955L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        lineLength.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -978374411721511888L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "appears " + t._2 + " times.");
            }
        });
    }
}
