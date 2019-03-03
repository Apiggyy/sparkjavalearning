package com.self.relearning.core;

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

        int factor = 3;
        Broadcast<Integer> broadFactor = sc.broadcast(factor);
        List<Integer> numerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numerList);
        JavaRDD<Integer> newNumbers = numbers.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 6809834364276354544L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * broadFactor.getValue();
            }
        });
        newNumbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -6754975255954624161L;

            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
        sc.close();
    }
}
