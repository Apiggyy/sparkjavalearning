package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> usersDF = sqlContext.read().load("H:/download/test/users.parquet");
        //usersDF.printSchema();
        //usersDF.show();
        usersDF.select("name","favorite_color").write().save("H:/download/test/nameAndFavoriteColor.parquet");
        sc.close();
    }
}
