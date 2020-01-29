package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.net.URI;

public class Korablic {
    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user"), conf);
        Path path = new Path("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user");
        if (hdfs.exists(path)){
            hdfs.delete(path,true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Success")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("hdfs://cdh631.itfbgroup.local:8020/user/usertest/event_data_train/event_data_train.csv");

        long count_steps = dataset.select("step_id").distinct().count();

        Dataset<Row> dataset1 = dataset.select("step_id","action","user_id")
                .where(dataset.col("action").like("passed"))
                .groupBy("user_id")
                .count();

        dataset1.where(String.format("count= %d",count_steps))
                .drop("count")
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user");

        sparkSession.stop();
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        double seconds = (double)totalTime / 1_000_000_000.0;
        System.out.println(seconds);
    }
}
