package com.self.relearning.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccmulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccmulatorVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        LongAccumulator sum = sc.sc().longAccumulator("sum");
        numbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -291696471195436055L;

            @Override
            public void call(Integer val) throws Exception {
                sum.add(val);
            }
        });
        System.out.println(sum.value());
        sc.close();
    }
}
