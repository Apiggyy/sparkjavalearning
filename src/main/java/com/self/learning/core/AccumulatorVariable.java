package com.self.learning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        LongAccumulator sum = sc.sc().longAccumulator("sum");
        numbersRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -1422152723006418702L;

            @Override
            public void call(Integer t) throws Exception {
                sum.add(t);
            }
        });
        System.out.println("累加值为：" + sum.value());
        sc.close();
    }
}
