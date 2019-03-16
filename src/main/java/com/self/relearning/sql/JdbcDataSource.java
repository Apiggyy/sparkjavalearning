package com.self.relearning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JdbcDataSource");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://spark1:3306/test");
        options.put("dbtable", "student_infos");
        Dataset<Row> studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");
        Dataset<Row> studentScoresDF = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Integer> studentInfosPairRDD = studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String,
                Integer>() {
            private static final long serialVersionUID = -8837047664957350232L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });
        JavaPairRDD<String, Integer> studentScoresPairRDD = studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String,
                Integer>() {
            private static final long serialVersionUID = -1716540324766648707L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfosPairRDD.join(studentScoresPairRDD);
        JavaRDD<Row> studentsRowRDD = studentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long serialVersionUID = 5212950845968543072L;

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });
        JavaRDD<Row> studentsFilterRowRDD = studentsRowRDD.filter(new Function<Row, Boolean>() {
            private static final long serialVersionUID = -4409991991242430550L;

            @Override
            public Boolean call(Row row) throws Exception {
                return row.getInt(2) > 80;
            }
        });
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> studentsDF = sqlContext.createDataFrame(studentsFilterRowRDD, structType);
        options.put("dbtable", "goodStudentInfos");
        studentsDF.write().mode(SaveMode.Overwrite).format("jdbc").options(options).saveAsTable("students");
        sc.close();
    }
}
