package hu.gulyasm.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.GroupedDataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Dataset Hello");
        conf.setSparkHome("/opt/spark-1.6.1-bin-hadoop2.6");
        conf.setMaster("local[4]");
        JavaSparkContext context = new JavaSparkContext(conf);
        String path = "/home/gulyasm/lmiss10.txt";
        SQLContext scontext = new SQLContext(context);
        Dataset<String> text = scontext.read().text(path).as(Encoders.STRING());

        Dataset<String> lines = text.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }, Encoders.STRING());

        GroupedDataset<String, String> grouped = lines.groupBy(new MapFunction<String, String>() {
            public String call(String value) throws Exception {
                return value.toLowerCase();
            }
        }, Encoders.STRING());

        Dataset<Tuple2<String, Object>> result= grouped.count();
        result.toDF().write().json("/home/gulyasm/workspace/spark-json");



        /*
        // =========== RDD API

        JavaRDD<String> lines2 = context.textFile(path);

        JavaRDD<String> words2 = lines2.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaRDD<String> lower2 = words2.map(new Function<String, String>() {
            public String call(String v1) throws Exception {
                return v1.toLowerCase();
            }
        });

        Map<String, Long> myMap = lower2.countByValue();

        JavaPairRDD<String, Integer> ones= lower2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });




        JavaPairRDD<String, Integer> result = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        result.saveAsTextFile("/home/gulyasm/workspace/spark1");
        */
    }

}
