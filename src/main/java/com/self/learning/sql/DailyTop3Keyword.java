package com.self.learning.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3Keyword {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DailyTop3Keyword");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> kwRDD = sc.textFile("hdfs://spark1:9000/spark-learning/keyword.txt");
        HiveContext hiveContext = new HiveContext(sc.sc());

        //伪造一份查询条件数据
        Map<String, List<String>> queryParamMap = new HashMap<>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sc.broadcast(queryParamMap);
        JavaRDD<String> filterKwRDD = kwRDD.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = -9188139619311144876L;

            @Override
            public Boolean call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();
                List<String> cities = queryParamMap.get("city");
                if (cities.size() > 0 && !cities.contains(city)) {
                    return false;
                }
                List<String> platforms = queryParamMap.get("platform");
                if (platforms.size() > 0 && !platforms.contains(platform)) {
                    return false;
                }
                List<String> versions = queryParamMap.get("version");
                if (versions.size() > 0 && !versions.contains(version)) {
                    return false;
                }
                return true;
            }
        });
        JavaPairRDD<String, String> dateKeywordUserRDD = filterKwRDD.mapToPair(new PairFunction<String, String,
                String>() {
            private static final long serialVersionUID = -4091215935419748010L;

            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String date = logSplited[0];
                String user = logSplited[1];
                String keyword = logSplited[2];
                return new Tuple2<>(date + "_" + keyword, user);
            }
        });
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            private static final long serialVersionUID = 7135020469771068376L;

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String dailyKeyword = t._1;
                Iterator<String> iterator = t._2.iterator();
                List<String> distinctUsers = new ArrayList<>();
                while (iterator.hasNext()) {
                    String user = iterator.next();
                    if (!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }
                long uv = distinctUsers.size();
                return new Tuple2<>(dailyKeyword, uv);
            }
        });
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            private static final long serialVersionUID = 7985296734089604779L;

            @Override
            public Row call(Tuple2<String, Long> t) throws Exception {
                String date = t._1.split("_")[0];
                String keyword = t._1.split("_")[1];
                long uv = t._2;
                return RowFactory.create(date, keyword, uv);
            }
        });
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dateKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRowRDD, structType);
        dateKeywordUvDF.registerTempTable("daily_keyword_uv");
        Dataset<Row> dailyTop3KeywordDF = hiveContext.sql("select date,keyword,uv from (\n" +
                "select date,keyword,uv,row_number() over(partition by date order by uv desc) rn from " +
                "daily_keyword_uv\n" +
                ") t where t.rn<=3");

        JavaPairRDD<String, String> top3DailyKeywordUvRDD = dailyTop3KeywordDF.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -7272940630523030270L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String date = String.valueOf(row.get(0));
                String keyword = String.valueOf(row.get(1));
                long uv = Long.valueOf(String.valueOf(row.get(2)));
                return new Tuple2<>(date, keyword + "_" + uv);
            }
        });

        JavaPairRDD<String, Iterable<String>> top3DailyKeywordUvPairsRDD = top3DailyKeywordUvRDD.groupByKey();
        JavaPairRDD<Long, String> UvDailyKeywordsPairsRDD = top3DailyKeywordUvPairsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            private static final long serialVersionUID = -4179625127938211461L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String date = t._1;
                Long totalUv = 0L;
                StringBuilder dateKeywords = new StringBuilder(date);
                Iterator<String> keywordUvIterator = t._2.iterator();
                while (keywordUvIterator.hasNext()) {
                    String keywordUv = keywordUvIterator.next();
                    totalUv += Long.valueOf(keywordUv.split("_")[1]);
                    dateKeywords.append(",").append(keywordUv);
                }
                return new Tuple2<>(totalUv, dateKeywords.toString());
            }
        });
        JavaPairRDD<Long, String> sortedParisRDD = UvDailyKeywordsPairsRDD.sortByKey(false);
        JavaRDD<Row> sortedRowsRDD = sortedParisRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            private static final long serialVersionUID = -7010705673634112653L;

            @Override
            public Iterator<Row> call(Tuple2<Long, String> t) throws Exception {
                String dailyKeywords = t._2;
                String date = dailyKeywords.split(",")[0];
                List<Row> rows = new ArrayList<>();
                rows.add(RowFactory.create(date, dailyKeywords.split(",")[1].split("_")[0], dailyKeywords.split(",")
                        [1].split("_")[1]));
                rows.add(RowFactory.create(date, dailyKeywords.split(",")[2].split("_")[0], dailyKeywords.split(",")
                        [2].split("_")[1]));
                rows.add(RowFactory.create(date, dailyKeywords.split(",")[3].split("_")[0], dailyKeywords.split(",")
                        [3].split("_")[1]));
                return rows.iterator();
            }
        });
        List<StructField> sortedStructFieds = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        );
        StructType sortedStructType = DataTypes.createStructType(sortedStructFieds);
        Dataset<Row> finalDF = hiveContext.createDataFrame(sortedRowsRDD, sortedStructType);
        finalDF.write().mode("overwrite").saveAsTable("daily_top3_keyword_uv");
        sc.close();
    }
}
