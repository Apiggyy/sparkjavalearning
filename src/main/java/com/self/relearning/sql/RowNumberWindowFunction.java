package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (product string, category string, revenu bigint)");
        hiveContext.sql("load data local inpath '/opt/spark-learning/resources/sales.txt' overwrite into table sales");
        Dataset<Row> top3SalesDF = hiveContext.sql("select product,category,revenu from (select a.*,row_number() over" +
                "(partition by category " +
                "order by revenu desc) rn from sales a) t where t.rn<=3");
        hiveContext.sql("drop table if exists top3_sales");
        top3SalesDF.write().mode(SaveMode.Overwrite).saveAsTable("top3_sales");
        sc.close();
    }
}
