package com.harvard.demo;

import org.apache.spark.sql.SparkSession;

public class HealthFacilitiesAnalysis {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Health Facilities in Ghana")
                .master("local[*]")
                .getOrCreate();

        spark
                .read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("data/health-facilities-gh.csv")
                .select("Region", "District", "Type", "Ownership")
                .groupBy("Region", "District", "Type", "Ownership")
                .count()
                .orderBy("Region", "District")
                .show();
    }
}
