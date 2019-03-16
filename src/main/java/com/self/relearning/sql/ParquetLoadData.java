package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> usersDF = sqlContext
                .read()
                .parquet("H:/download/test/users.parquet");
        usersDF.createOrReplaceTempView("users");
        Dataset<Row> namesDF = sqlContext.sql("select name from users");
        List<String> names = namesDF.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = 3984479194440099932L;

            @Override
            public String call(Row row) throws Exception {
                return "Name : " + row.getString(0);
            }
        }).collect();
        for (String name : names) {
            System.out.println(name);
        }
        sc.close();
    }
}
