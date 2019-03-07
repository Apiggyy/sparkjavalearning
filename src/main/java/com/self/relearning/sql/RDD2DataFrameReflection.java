package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("H:/download/test/students.txt");
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = 5775462044959650273L;

            @Override
            public Student call(String line) throws Exception {
                return new Student(Integer.valueOf(line.split(",")[0]), line.split(",")[1], Integer.valueOf(line
                        .split(",")[2]));
            }
        });
        Dataset<Row> studentsDF = sqlContext.createDataFrame(students, Student.class);
        studentsDF.registerTempTable("students");
        Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age < 18");
        JavaRDD<Row> teeagerRDD = teenagerDF.javaRDD();
        JavaRDD<Student> teenager = teeagerRDD.map(new Function<Row, Student>() {
            private static final long serialVersionUID = -3602488823428242243L;

            @Override
            public Student call(Row row) throws Exception {
                return new Student(row.getInt(1), row.getString(2), row.getInt(0));
            }
        });
        List<Student> studentList = teenager.collect();
        for (Student student : studentList) {
            System.out.println(student);
        }
        sc.close();
    }
}
