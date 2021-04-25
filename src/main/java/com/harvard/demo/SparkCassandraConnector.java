package com.harvard.demo;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkCassandraConnector {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Cassandra Connector")
                .master("local[*]")
                .config("spark.files","D:\\secure-connect-democassandra.zip")
                .config("spark.cassandra.connection.config.cloud.path","secure-connect-democassandra.zip")
                .getOrCreate();

        spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("data/ml-latest-small/movies.csv")
                .withColumnRenamed("movieId", "movieid")
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "spark")
                .option("table", "movies")
                .option("spark.cassandra.auth.username", "YGwOypFXOSvYUrhzmFJyIKIF")
                .option("spark.cassandra.auth.password", "+duG7c6Iky_SWS2-1kGwxBn4Y7.d2BBuEsSdicpDfWLiW7_lfLZOezo2cJi2RN1dQZ2dZY3zp_SlgcJ-vnEzjggqm7FlnOkCaquUpfhAgXK1h9NASZLrp41x,p7YjGQp")
                .mode(SaveMode.Append)
                .save();
    }
}
