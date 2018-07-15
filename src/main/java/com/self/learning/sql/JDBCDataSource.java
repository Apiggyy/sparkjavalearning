package com.self.learning.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JDBCDataSource {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JDBCDataSource")
                .config("spark.testing.memory", "1073741824")
                .getOrCreate();

        String url = "jdbc:mysql://192.168.1.194:3306/test";
        Properties connProp = new Properties();
        connProp.put("user", "root");
        connProp.put("password", "1234");
        connProp.put("driver", "com.mysql.jdbc.Driver");
        Dataset<Row> studentInfosDF = spark.read().jdbc(url, "student_infos", connProp);

        Dataset<Row> studentScoresDF = spark.read().jdbc(url, "student_scores", connProp);

        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfosDF.javaRDD().mapToPair(new PairFunction<Row,
                String, Integer>() {
            private static final long serialVersionUID = 1700917665872058537L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 924520909777429718L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        JavaRDD<Row> studentsRowRDD = studentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {

            private static final long serialVersionUID = -8643642892960067372L;

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });

        JavaRDD<Row> goodStudentsRowRDD = studentsRowRDD.filter(new Function<Row, Boolean>() {
            private static final long serialVersionUID = -3471591174838235571L;

            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;
            }
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDF = spark.createDataFrame(goodStudentsRowRDD, structType);
        goodStudentsDF.write().mode("overwrite").jdbc(url, "good_student_infos", connProp);

        spark.stop();

    }
}
