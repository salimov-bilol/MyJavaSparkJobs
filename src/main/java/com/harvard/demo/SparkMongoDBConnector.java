package com.harvard.demo;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkMongoDBConnector {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Apache Spark MongoDB Connector")
                .config("spark.mongodb.input.uri", "mongodb+srv://wiut00007072:wiut00007072@00007072.gwori.mongodb.net/g-shop.users")
                .getOrCreate();

        spark
                .read()
                .format("mongo")
                .load()
                .select("_id.oid", "email", "isAdmin", "name", "password")
                .write()
                .format("jdbc")
                .option("url", "jdbc:mysql://database-1.cyn4i0c19grl.us-east-1.rds.amazonaws.com:3306")
                .option("dbtable", "spark.users")
                .option("user", "root")
                .option("password", "harvard12345")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
