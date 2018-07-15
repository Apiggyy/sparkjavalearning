package com.self.learning.sql;

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
        hiveContext.sql("create table if not exists sales(product string , category string, revenu bigint)");
        hiveContext.sql("load data local inpath '/opt/software/spark-learning/java/sql/sales.txt' into table sales");

        Dataset<Row> top1sales = hiveContext.sql("select product,category,revenu from (" +
                "select product,category,revenu,row_number() over(partition by category order by revenu desc) rn from sales" +
                ") t where t.rn=1");

        hiveContext.sql("drop table if exists top1sales");
        top1sales.write().mode(SaveMode.Overwrite).saveAsTable("top1sales");

        hiveContext.table("top1sales");

        sc.close();
    }
}
