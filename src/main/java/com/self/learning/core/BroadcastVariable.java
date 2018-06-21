package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadcastVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int factor = 3;
        Broadcast<Integer> broadcastFactor = sc.broadcast(factor);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        JavaRDD<Integer> multiNumbers = numbersRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 3281299632089437996L;

            @Override
            public Integer call(Integer v1) throws Exception {
                int factor = broadcastFactor.value();
                return v1 * factor;
            }
        });
        multiNumbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 6246707110314516008L;

            @Override
            public void call(Integer v1) throws Exception {
                System.out.println(v1);
            }
        });
        sc.close();


    }
}
