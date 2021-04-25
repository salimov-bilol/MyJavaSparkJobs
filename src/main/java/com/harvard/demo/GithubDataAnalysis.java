package com.harvard.demo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class GithubDataAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Github Dataset Analysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> githubData = spark
                .read()
                .option("inferSchema", "true")
                .json("data/github/2015-01-01-15.json")
                .filter("type = 'PushEvent'")
                .groupBy(col("actor.login").as("actor"))
                .count()
                .orderBy(col("count").desc());

        Set<String> employees = null;
        try {
            employees = Files
                    .lines(Paths.get("data/github/ghEmployees.txt"))
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Broadcast<Set<String>> brEmployees = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .broadcast(employees);

        spark.udf().register("myUDF", udf((String s) -> brEmployees.getValue().contains(s), DataTypes.BooleanType));

        Dataset<Row> afterFiltering = githubData.filter(callUDF("myUDF", col("actor")));

        afterFiltering.show();
    }
}
