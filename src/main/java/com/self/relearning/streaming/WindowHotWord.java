package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class WindowHotWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("WindowHotWord")
                .setMaster("local[2]");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //leo hello
        //tom world
        JavaReceiverInputDStream<String> searchLogsDStream = jssc.socketTextStream("spark1", 9999);
        JavaDStream<String> searchWordsDStream = searchLogsDStream.map(new Function<String, String>() {
            private static final long serialVersionUID = 2162402564265380874L;

            @Override
            public String call(String searchLog) throws Exception {
                return searchLog.split(" ")[1];
            }
        });
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordsDStream.mapToPair(new PairFunction<String, String,
                Integer>() {
            private static final long serialVersionUID = 7681630456882809577L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        JavaPairDStream<String, Integer> searchWordCountsDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -6292682039968633204L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));
        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 722554611023466037L;

            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws
                    Exception {
                JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    private static final long serialVersionUID = 5625052704208689757L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }
                });
                JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false);
                JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    private static final long serialVersionUID = -2833154802965895441L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }
                });
                List<Tuple2<String, Integer>> hotSearchWordCounts = sortedSearchWordCountsRDD.take(3);
                for (Tuple2<String, Integer> hotSearchWordCount : hotSearchWordCounts) {
                    System.out.println(hotSearchWordCount._1 + " appears " + hotSearchWordCount._2 + " times.");
                }
                return sortedSearchWordCountsRDD;
            }
        });
        //只是为了触发job的执行，必须有output操作
        finalDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
