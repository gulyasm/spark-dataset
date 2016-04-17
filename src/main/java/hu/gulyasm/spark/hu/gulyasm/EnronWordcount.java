package hu.gulyasm.spark.hu.gulyasm;

import hu.gulyasm.EnronEmail;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class EnronWordcount {

    public static void main(String[] args) {
        JavaSparkContext context = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("EWC").setSparkHome("spark-1.6.1-bin-hadoop2.6"));

        SQLContext sc = new SQLContext(context);
        Dataset<EnronEmail> emails = sc.read().json("/home/gulyasm/workspace/json-data/enron.json").as(Encoders.bean(EnronEmail.class));
        Dataset<String> words = emails.flatMap(new FlatMapFunction<EnronEmail, String>() {
            public Iterable<String> call(EnronEmail enronEmail) throws Exception {
                return Arrays.asList(enronEmail.getText().split(" "));
            }
        }, Encoders.STRING());
        Dataset<String> lowWords = words.map(new MapFunction<String, String>() {

            public String call(String value) throws Exception {
                return value.toLowerCase();
            }
        }, Encoders.STRING());

        lowWords.groupBy(new MapFunction<String, String>() {
            public String call(String value) throws Exception {
                return value;
            }
        }, Encoders.STRING()).count().toDF().write().json("/home/gulyasm/workspace/json-data/result-3");
    }
}
