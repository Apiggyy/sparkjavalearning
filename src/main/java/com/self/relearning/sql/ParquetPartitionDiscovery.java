package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ParquetPartitionDiscovery {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery").setMaster("local");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> usersDF = sqlContext
                .read()
                .option("basepath","hdfs://spark1:9000/user")
                .parquet("hdfs://spark1:9000/user/gender=male/country=US/users.parquet");
        usersDF.printSchema();
        usersDF.show();
        sc.close();
    }
}
