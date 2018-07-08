package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrameToReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameToReflection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\students.txt");
        JavaRDD<Student> studentJavaRDD = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = -340915448939434492L;

            @Override
            public Student call(String line) throws Exception {
                String[] stuInfo = line.split(",");
                return new Student(Integer.parseInt(stuInfo[0]), stuInfo[1], Integer.parseInt(stuInfo[2]));
            }
        });

        Dataset<Row> df = sqlContext.createDataFrame(studentJavaRDD, Student.class);
        df.registerTempTable("students");
        Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age>=18");
        JavaRDD<Row> rows = teenagerDF.toJavaRDD();
        JavaRDD<Student> studentList = rows.map(new Function<Row, Student>() {

            private static final long serialVersionUID = 8749181098385279286L;

            @Override
            public Student call(Row row) throws Exception {
                Student student = new Student();
                student.setAge(row.getInt(0));
                student.setId(row.getInt(1));
                student.setName(row.getString(2));
                return student;
            }
        });
        List<Student> students = studentList.collect();
        for (Student student : students) {
            System.out.println(student);
        }

    }
}
