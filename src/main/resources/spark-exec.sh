/opt/software/spark/bin/spark-submit \
--class com.self.learning.sql.ParquetMergeSchema \
--num-executors 3  \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3  \
--files /opt/software/hive/conf/hive-site.xml \
--driver-class-path /opt/software/spark-learning/java/lib/mysql-connector-java-5.1.46.jar \
/opt/software/spark-learning/scala/sql/sparkscalalearning-1.0-SNAPSHOT.jar
