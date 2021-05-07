package com.harvard.demo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class MovieRatingsAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Movies Dataset Analysis")
                .master("local[*]")
                .config("spark.files","D:\\secure-connect-democassandra.zip")
                .config("spark.cassandra.connection.config.cloud.path","secure-connect-democassandra.zip")
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

        spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/ml-latest-small/movies.csv")
                .join(ratings, "movieId")
                .select("movieId", "title", "ratings_count", "ratings_avg")
                .filter("ratings_count > 250")
                .filter("ratings_avg > 4")
                .withColumnRenamed("movieId","movie_id")
                .sort(col("ratings_avg").desc())
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "spark")
                .option("table", "movies_analytics")
                .option("spark.cassandra.auth.username", "YGwOypFXOSvYUrhzmFJyIKIF")
                .option("spark.cassandra.auth.password", "+duG7c6Iky_SWS2-1kGwxBn4Y7.d2BBuEsSdicpDfWLiW7_lfLZOezo2cJi2RN1dQZ2dZY3zp_SlgcJ-vnEzjggqm7FlnOkCaquUpfhAgXK1h9NASZLrp41x,p7YjGQp")
                .mode(SaveMode.Append)
                .save();

    }
}
