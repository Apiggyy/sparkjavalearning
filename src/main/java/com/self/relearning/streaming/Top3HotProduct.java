package com.self.relearning.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("PersistWordCount")
                .setMaster("local[2]");
        conf.set("spark.testing.memory", "1073741824");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> productClickLogsDStream = jsc.socketTextStream("spark1", 9999);
        JavaPairDStream<String, Integer> categoryProductPairsDStream = productClickLogsDStream.mapToPair(new PairFunction<String,
                String, Integer>() {
            private static final long serialVersionUID = -6605680282335199593L;

            @Override
            public Tuple2<String, Integer> call(String productClickLog) throws Exception {
                return new Tuple2<>(productClickLog.split(" ")[2] + "_" + productClickLog.split(" ")[1], 1);
            }
        });
        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairsDStream
                .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -6493888508882343360L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));
        categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = -3536232660037955375L;

            @Override
            public void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                JavaRDD<Row> categoryProductCountsRowRDD = categoryProductCountsRDD.map(new Function<Tuple2<String,
                        Integer>, Row>() {
                    private static final long serialVersionUID = 5274981433552085716L;

                    @Override
                    public Row call(Tuple2<String, Integer> t) throws Exception {
                        String category = t._1.split("_")[0];
                        String product = t._1.split("_")[1];
                        Integer count = t._2;
                        return RowFactory.create(category, product, count);
                    }
                });
                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                StructType structType = DataTypes.createStructType(structFields);
                SQLContext sqlContext = new SQLContext(categoryProductCountsRDD.context());
                Dataset<Row> categoryProductCountsDF = sqlContext.createDataFrame(categoryProductCountsRowRDD, structType);
                categoryProductCountsDF.createOrReplaceTempView("product_click_log");
                Dataset<Row> top3ProductDF = sqlContext.sql("select category,product,click_count from (" +
                        "select a.*,row_number() over(partition by category order by click_count desc) rn " +
                        "from product_click_log a) t " +
                        "where t.rn<=3");
                top3ProductDF.show();
            }
        });
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
