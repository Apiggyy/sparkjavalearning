package com.self.learning.sql;

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
        SparkConf conf = new SparkConf().setAppName("JsonDataSource");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> df = sqlContext.read().json("hdfs://spark1:9000/spark-learning/students.json");
        df.registerTempTable("students_scores");
        Dataset<Row> goodStudentsScoreDF = sqlContext.sql("select name,score from students_scores where score>=80");
        List<String> goodStudentsNames = goodStudentsScoreDF.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = -6539788716796624192L;

            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> studentsInfoJsons = new ArrayList<>();
        studentsInfoJsons.add("{\"name\":\"Leo\",\"age\":18}");
        studentsInfoJsons.add("{\"name\":\"Marry\",\"age\":17}");
        studentsInfoJsons.add("{\"name\":\"Jack\",\"age\":19}");

        JavaRDD<String> studentsInfoRDD = sc.parallelize(studentsInfoJsons);
        Dataset<Row> studentsInfoDF = sqlContext.read().json(studentsInfoRDD);
        studentsInfoDF.registerTempTable("students_info");
        String sql = "select name,age from students_info where name in (";
        for (int i = 0; i < goodStudentsNames.size(); i++) {
            sql += "'" + goodStudentsNames.get(i) + "'";
            if(i < goodStudentsNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";
        Dataset<Row> goodStudentsInfoDF = sqlContext.sql(sql);
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                goodStudentsScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 3682915691422816849L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(goodStudentsInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = -5563926165611601776L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));
        JavaRDD<Row> goodStudentsRowRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long serialVersionUID = 6220418575479439548L;

            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDF = sqlContext.createDataFrame(goodStudentsRowRDD, structType);
        goodStudentsDF.write().format("json").save("hdfs://spark1:9000/spark-learning/good_students.json");
    }
}
