package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameOperation");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.read().json("hdfs://spark1:9000/user/data/students.json");
        df.show();
        df.schema();
        df.select("name").show();
        df.select(df.col("name"), df.col("age").plus(1)).show();
        df.filter(df.col("age").gt(18)).show();
        df.groupBy(df.col("age")).count().show();
        sc.close();
    }
}
