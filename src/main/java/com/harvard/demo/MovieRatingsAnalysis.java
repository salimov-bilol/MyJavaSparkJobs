package com.harvard.demo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class MovieRatingsAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Movies Dataset Analysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> ratings = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/ml-latest-small/ratings.csv")
                .select("movieId", "rating")
                .groupBy("movieId")
                .agg(count("rating").as("ratings_count"),
                        avg("rating").as("ratings_avg"));

        Dataset<Row> movies = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/ml-latest-small/movies.csv")
                .join(ratings, "movieId")
                .select("title", "ratings_count", "ratings_avg")
                .filter("ratings_count > 250")
                .filter("ratings_avg > 4")
                .sort(col("ratings_avg").desc());

        movies.show(false);

    }
}
