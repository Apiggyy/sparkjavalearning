package com.self.learning.sql;

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

public class RDD2DataFrameProgrammaticlly {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameProgrammaticlly").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("E:\\迅雷下载\\students.txt");
        JavaRDD<Row> studentsRDD = lines.map(new Function<String, Row>() {
            private static final long serialVersionUID = -2923285818161866480L;

            @Override
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                return RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));
            }
        });

        //动态构造元数据/
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> df = sqlContext.createDataFrame(studentsRDD, structType);
            df.registerTempTable("students");
        Dataset<Row> students = sqlContext.sql("select * from students where age>=18");
        List<Row> rows = students.toJavaRDD().collect();
        for (Row row : rows) {
            System.out.println(row);
        }
        sc.close();

    }

}
