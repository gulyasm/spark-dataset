package hu.gulyasm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class Enron {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Enron");
        conf.setSparkHome("/opt/spark-1.6.1-bin-hadoop2.6");

        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(context);

        Dataset<EnronEmail> emails = sql.read().json("/home/gulyasm/workspace/json-data/enron.json").as(Encoders.bean(EnronEmail.class));
        GroupedDataset<String, EnronEmail> grouped = emails.groupBy(new MapFunction<EnronEmail, String>() {
            public String call(EnronEmail value) throws Exception {
                return value.getSender();
            }
        }, Encoders.STRING());

        DataFrame resultDF = grouped.count().toDF();
        DataFrame resultDF2 = resultDF.withColumnRenamed("count(1)", "count");
        resultDF2.write().json("/home/gulyasm/workspace/json-data/result-2");
    }
}
