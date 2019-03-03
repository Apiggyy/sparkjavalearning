package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("H:/download/test/test03.txt");
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            private static final long serialVersionUID = -3132851270213106619L;

            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                return new Tuple2<>(new SecondarySortKey(Integer.valueOf(line.split(" ")[0]), Integer.valueOf(line
                        .split(" ")[1])), line);
            }
        });
        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey(false);
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            private static final long serialVersionUID = 1181135449499447479L;

            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        sortedLines.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -1095758057902226319L;

            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
        sc.close();
    }
}
