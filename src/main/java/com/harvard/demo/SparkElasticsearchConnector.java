package com.harvard.demo;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.functions.*;

public class SparkElasticsearchConnector {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Elasticsearch Connector")
                .master("local[*]")
                .config("spark.es.nodes", "https://elastic:fgyNN8z6xcC9BPVjL9zTXtRW@elasticsearch-demo.es.us-east-1.aws.found.io")
                .config("spark.es.port", "9243")
                .config("spark.es.nodes.wan.only", "true")
                .getOrCreate();

        var schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("maker", StringType, true),
                DataTypes.createStructField("model", StringType, true),
                DataTypes.createStructField("mileage", IntegerType, true),
                DataTypes.createStructField("manufacture_year", StringType, true),
                DataTypes.createStructField("engine_displacement", StringType, true),
                DataTypes.createStructField("engine_power", IntegerType, true),
                DataTypes.createStructField("body_type", StringType, true),
                DataTypes.createStructField("color_slug", StringType, true),
                DataTypes.createStructField("stk_year", StringType, true),
                DataTypes.createStructField("transmission", StringType, true),
                DataTypes.createStructField("door_count", IntegerType, true),
                DataTypes.createStructField("seat_count", IntegerType, true),
                DataTypes.createStructField("fuel_type", StringType, true),
                DataTypes.createStructField("date_created", StringType, true),
                DataTypes.createStructField("date_last_seen", StringType, true),
                DataTypes.createStructField("price_eur", DoubleType, true)
        ));

        spark
                .read()
                .option("header", "true")
                .schema(schema)
                .csv("data/cars.csv")
                .groupBy("maker", "fuel_type")
                .agg(
                        avg("price_eur").as("avg_price_eur"),
                        count("*").as("count"))
                .select("maker", "fuel_type", "avg_price_eur", "count")
                .orderBy(col("avg_price_eur").desc())
                .write()
                .format("org.elasticsearch.spark.sql")
                .option("es.resource", "cars_analytics")
                .option("es.net.http.auth.user", "elastic")
                .option("es.net.http.auth.pass", "fgyNN8z6xcC9BPVjL9zTXtRW")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
