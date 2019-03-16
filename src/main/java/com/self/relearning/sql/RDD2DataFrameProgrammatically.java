package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("H:/download/test/students.txt");
        JavaRDD<Row> studentsRDD = lines.map(new Function<String, Row>() {
            private static final long serialVersionUID = 2348236122734221164L;

            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");
                return RowFactory.create(Integer.valueOf(splited[0]), splited[1], Integer.valueOf(splited[2]));
            }
        });
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> studentsDF = sqlContext.createDataFrame(studentsRDD, structType);
        studentsDF.registerTempTable("students");
        Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age < 18");
        List<Row> rowList = teenagerDF.javaRDD().collect();
        for (Row row : rowList) {
            System.out.println(row);
        }
    }
}
