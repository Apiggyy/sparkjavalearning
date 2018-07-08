package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ManullySpecifyOperations {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GenericLoadAndSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> df = sqlContext.read().format("json").load("E:\\迅雷下载\\people.json");
        df.select("name").write().format("parquet").save("E:\\迅雷下载\\peopleName.parquet");
        sc.close();
    }
}
