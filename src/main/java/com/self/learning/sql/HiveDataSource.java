package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists student_infos");
        hiveContext.sql("create table if not exists student_infos (name string, age int)");
        hiveContext.sql("load data local inpath '/opt/software/spark-learning/resources/student_infos.txt' into table student_infos");

        hiveContext.sql("drop table if exists student_scores");
        hiveContext.sql("create table if not exists student_scores(name string, score int)");
        hiveContext.sql("load data local inpath '/opt/software/spark-learning/resources/student_scores.txt' into table student_scores");

        Dataset<Row> goodStudensDF = hiveContext.sql("select a.name,a.age,b.score from student_infos a inner join " +
                "student_scores b on a.name=b" +
                ".name where b.score >= 80");

        hiveContext.sql("drop table if exists good_student_infos");
        goodStudensDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");

        hiveContext.table("good_student_infos").show();

        sc.close();
    }
}
