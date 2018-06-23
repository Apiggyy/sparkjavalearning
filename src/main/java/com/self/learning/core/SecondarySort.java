package com.self.learning.core;

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

        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\sort.txt");
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            private static final long serialVersionUID = 695328652904714682L;

            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineSplit = line.split(" ");
                SecondarySortKey sort = new SecondarySortKey(
                        Integer.valueOf(lineSplit[0]),
                        Integer.valueOf(lineSplit[1]));
                return new Tuple2<>(sort, line);
            }
        });
        JavaPairRDD<SecondarySortKey, String> sortPairs = pairs.sortByKey();
        JavaRDD<String> sortLines = sortPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            private static final long serialVersionUID = 568882489501529571L;

            @Override
            public String call(Tuple2<SecondarySortKey, String> t) throws Exception {
                return t._2;
            }
        });
        sortLines.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -158933584045855469L;

            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
        sc.close();
    }
}
