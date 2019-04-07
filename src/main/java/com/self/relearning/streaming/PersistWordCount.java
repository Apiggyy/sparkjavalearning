package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PersistWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("PersistWordCount")
                .setMaster("local[2]");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        //开启checkpoint机制
        jsc.checkpoint("hdfs://spark1:9000//user/sparklearning/checkpoint");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("spark1", 9999);
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>,
                Optional<Integer>, Optional<Integer>>() {
            private static final long serialVersionUID = -7179023551072960776L;

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if (state.isPresent()) {
                    //上一次状态的值
                    newValue = state.get();
                }
                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        });
        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 9105986029469491752L;

            @Override
            public void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    private static final long serialVersionUID = 5460813633720741383L;

                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        Connection conn = ConnectionPool.getConnection();
                        while (wordCounts.hasNext()) {
                            Tuple2<String, Integer> wordCount = wordCounts.next();
                            String sql = "insert into wordcount (word,count) values ('" +
                                    wordCount._1 + "'," + wordCount._2 + ");";
                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }
                        ConnectionPool.returnConnection(conn);
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
