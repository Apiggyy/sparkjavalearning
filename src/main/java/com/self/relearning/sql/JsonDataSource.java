package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JsonDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> studentsDF = sqlContext.read().json("hdfs://spark1:9000/user/students.json");
        studentsDF.createOrReplaceTempView("student_scores");
        Dataset<Row> goodStudentNamesDF = sqlContext.sql("select name,score from student_scores where score>=80");
        List<String> names = goodStudentNamesDF.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = -7129137887122299235L;

            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();
        List<String> studentInfoJsons = new ArrayList<>();
        studentInfoJsons.add("{\"name\":\"Leo\",\"age\":18}");
        studentInfoJsons.add("{\"name\":\"Marry\",\"age\":22}");
        studentInfoJsons.add("{\"name\":\"Jack\",\"age\":30}");
        JavaRDD<String> studentInfoJsonsRDD = sc.parallelize(studentInfoJsons);
        Dataset<Row> studentInfoJsonsDF = sqlContext.read().json(studentInfoJsonsRDD);
        studentInfoJsonsDF.createOrReplaceTempView("student_infos");
        StringBuilder sql = new StringBuilder("select name,age from student_infos where name in (");
        for (String name : names) {
            sql.append("'").append(name).append("'").append(",");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 1)).append(")");
        Dataset<Row> goodStudentInfosDF = sqlContext.sql(sql.toString());
        JavaPairRDD<String, Integer> goodStudentNamesPair = goodStudentNamesDF.javaRDD().mapToPair(new PairFunction<Row, String,
                Integer>() {
            private static final long serialVersionUID = 6299675944273498707L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        });
        JavaPairRDD<String, Integer> goodStudentInfoPair = goodStudentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String,
                Integer>() {
            private static final long serialVersionUID = 1291928893509628738L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        });
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsPairRDD = goodStudentInfoPair.join(goodStudentNamesPair);

        JavaRDD<Row> goodStudentRowRDD = goodStudentsPairRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long serialVersionUID = -5518631594567317257L;

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> goodStudentsDF = sqlContext.createDataFrame(goodStudentRowRDD, structType);
        goodStudentsDF.write().format("json").save("hdfs://spark1:9000/user/goodStudents");
        sc.close();
    }
}
