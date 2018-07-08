package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameCreate {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreate");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> rows = sqlContext.read().json("hdfs://spark1:9000/user/data/students.json");
        rows.show();
        sc.close();
    }
}
