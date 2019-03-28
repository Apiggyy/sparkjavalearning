package com.self.relearning.sql;

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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 每日top3热点搜索词统计案列
 */
public class DailyTop3Keyword {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction");
        conf.set("spark.testing.memory", "1073741824");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        Map<String, List<String>> queryParamMap = new HashMap<>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));
        JavaRDD<String> orgRDD = sc.textFile("hdfs://spark1:9000/user/sparklearning/keyword.txt");
        Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sc.broadcast(queryParamMap);
        JavaRDD<String> filterOrgRDD = orgRDD.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = 4759959134166309213L;

            @Override
            public Boolean call(String line) throws Exception {
                String[] logSplited = line.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                Map<String, List<String>> queryParamMap = queryParamMapBroadcast.getValue();
                if (queryParamMap.get("city").size() > 0 && !queryParamMap.get("city").contains(city)) return false;
                if (queryParamMap.get("platform").size() > 0 && !queryParamMap.get("platform").contains(platform))
                    return false;
                if (queryParamMap.get("version").size() > 0 && !queryParamMap.get("version").contains(version))
                    return false;
                return true;
            }
        });
        JavaPairRDD<String, String> dateKeywordUserRDD = filterOrgRDD.mapToPair(new PairFunction<String, String,
                String>() {
            private static final long serialVersionUID = -3881352110922550940L;

            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<>(line.split("\t")[0] + "_" + line.split("\t")[2], line.split("\t")[1]);
            }
        });
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String,
                Iterable<String>>, String, Long>() {
            private static final long serialVersionUID = -856634725670070307L;

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String dateKeyword = t._1;
                Iterator<String> users = t._2.iterator();
                List<String> distinctUsers = new ArrayList<>();
                while (users.hasNext()) {
                    String user = users.next();
                    if (!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }
                long uv = distinctUsers.size();
                return new Tuple2<String, Long>(dateKeyword, uv);
            }
        });
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            private static final long serialVersionUID = -7225870628420316535L;

            @Override
            public Row call(Tuple2<String, Long> t) throws Exception {
                return RowFactory.create(t._1.split("_")[0], t._1.split("_")[1], t._2);
            }
        });
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dateKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRowRDD, structType);
        dateKeywordUvDF.createOrReplaceTempView("daily_keyword_uv");
        Dataset<Row> dailyTop3KeywordUvDF = hiveContext.sql("select date,keyword,uv from " +
                "(select date,keyword,uv,row_number() over(partition by date order by uv desc) rn from " +
                "daily_keyword_uv) t " +
                "where t.rn <= 3");
        JavaPairRDD<String, String> dailyTop3KeywordUvPairRDD = dailyTop3KeywordUvDF.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

            private static final long serialVersionUID = 117667467606097849L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getString(1) + "_" + row.getLong(2));
            }
        });
        JavaPairRDD<String, Iterable<String>> dailyTop3KeywordUvsPairRDD = dailyTop3KeywordUvPairRDD.groupByKey();
        JavaPairRDD<Long, String> dailyTotalUvKeywordPairRDD = dailyTop3KeywordUvsPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            private static final long serialVersionUID = 8200382808276573331L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                long totalUv = 0;
                String date = t._1;
                StringBuilder dateKeywords = new StringBuilder(date);
                for (String keywordUv : t._2) {
                    totalUv += Long.valueOf(keywordUv.split("_")[1]);
                    dateKeywords.append(",").append(keywordUv);
                }
                return new Tuple2<>(totalUv, dateKeywords.toString());
            }
        });
        JavaPairRDD<Long, String> sortedDailyTotalUvKeywordPairRDD = dailyTotalUvKeywordPairRDD.sortByKey(false);
        JavaRDD<Row> DailyTotalUvKeywordRowRDD = sortedDailyTotalUvKeywordPairRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>,
                Row>() {
            private static final long serialVersionUID = 1185580051570803786L;

            @Override
            public Iterator<Row> call(Tuple2<Long, String> t) throws Exception {
                String[] dateKeywordUv = t._2.split(",");
                String date = null;
                List<Row> rows = new ArrayList<>();
                for (int i = 0; i < dateKeywordUv.length; i++) {
                    if (i == 0) {
                        date = dateKeywordUv[0];
                    } else {
                        rows.add(RowFactory.create(date, dateKeywordUv[i].split("_")[0], Long.valueOf
                                (dateKeywordUv[i].split("_")[1])));
                    }
                }
                return rows.iterator();
            }
        });
        Dataset<Row> finalDF = hiveContext.createDataFrame(DailyTotalUvKeywordRowRDD, structType);
        finalDF.write().mode(SaveMode.Overwrite).saveAsTable("daily_top3_keyword_uv");
        sc.close();
    }
}
