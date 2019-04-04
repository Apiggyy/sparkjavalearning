package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformBlacklist {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("UpdateStateByKeyWordCount")
                .setMaster("local[2]");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //模拟黑名单
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
        blacklist.add(new Tuple2<>("tom", true));
        JavaPairRDD<String, Boolean> blacklistRDD = jssc.sparkContext().parallelizePairs(blacklist);
        
        JavaReceiverInputDStream<String> adsClickLog = jssc.socketTextStream("spark1", 9999);
        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLog.mapToPair(new PairFunction<String,
                String, String>() {
            private static final long serialVersionUID = -7581890142941382346L;

            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<>(adsClickLog.split(" ")[1], adsClickLog);
            }
        });

        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>,
                JavaRDD<String>>() {
            private static final long serialVersionUID = -1782459419178987587L;

            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRDD
                        .leftOuterJoin(blacklistRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    private static final long serialVersionUID = -4569966323048539329L;

                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> t) throws Exception {
                        if (t._2._2.isPresent() && t._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> validAdsClickLogRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String,
                        Optional<Boolean>>>,
                        String>() {
                    private static final long serialVersionUID = 2770273030725230160L;

                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> t) throws Exception {
                        return t._2._1;
                    }
                });
                return validAdsClickLogRDD;
            }
        });
        validAdsClickLogDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
