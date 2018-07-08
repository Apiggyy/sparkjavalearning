package com.self.learning.sql;

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

        Dataset<Row> df = sqlContext.read().parquet("E:\\迅雷下载\\users.parquet");
        df.registerTempTable("user");
        Dataset<Row> userNamesDF = sqlContext.sql("select name from user");
        List<String> userNames = userNamesDF.toJavaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = -501689911040003245L;

            @Override
            public String call(Row row) throws Exception {
                return "name: " + row.getString(0);
            }
        }).collect();
        for (String userName : userNames) {
            System.out.println(userName);
        }
        sc.close();
    }
}
