package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreate");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> ds = sqlContext.read().json("hdfs://spark1:9000/user/students.json");
        ds.printSchema();
        //查询某列数据
        ds.select("name").show();
        //查询某几列数据
        ds.select(ds.col("name"),ds.col("age").plus(1)).show();
        //根据某一列的值进行过滤
        ds.filter(ds.col("age").gt(18)).show();
        //根据某一列进行分组，然后进行聚合
        ds.groupBy(ds.col("age")).count().show();
        sc.close();
    }
}
