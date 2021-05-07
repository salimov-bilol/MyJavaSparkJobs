package com.harvard.demo;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class StructuredStreamingWordCount {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Structured Streaming WordCount")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.readStream().text("data/input");

        FlatMapFunction<String, String> flatMapFunction = x -> Arrays.stream(x.split(" ")).iterator();

        Dataset<String> words = df.as(Encoders.STRING()).flatMap(flatMapFunction, Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        wordCounts
                .writeStream()
                .format("console")
                .outputMode("complete")
                .start()
                .awaitTermination();
    }
}
