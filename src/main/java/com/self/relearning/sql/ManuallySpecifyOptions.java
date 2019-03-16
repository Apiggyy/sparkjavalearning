package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> peopleDF = sqlContext.read().format("json").load("H:/download/test/people.json");
        //peopleDF.printSchema();
        //peopleDF.show();
        peopleDF.select("name")
                .write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("H:/download/test/peopleName.parquet");
        sc.close();
    }
}
