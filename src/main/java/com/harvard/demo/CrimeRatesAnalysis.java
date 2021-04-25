package com.harvard.demo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.udf;

public class CrimeRatesAnalysis {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Crime Rates in Boston")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> crimes = spark
                .read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("data/crime-rates/crime.csv")
                .select("OFFENSE_CODE", "DISTRICT", "STREET")
                .na()
                .drop();

        List<Row> crimesCountByDistrictArray = crimes
                .groupBy("DISTRICT")
                .count().collectAsList();

        var crimesCountByDistrictMap = new HashMap<String, Long>();

        for (Row row : crimesCountByDistrictArray) {
            crimesCountByDistrictMap.put(row.getString(0), row.getLong(1));
        }

        Broadcast<HashMap<String, Long>> brCrimesCount = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .broadcast(crimesCountByDistrictMap);

        spark.udf().register("myUDF", udf((String s) -> brCrimesCount.getValue().getOrDefault(s, 0L), DataTypes.LongType));

        Dataset<Row> aggregation = crimes
                .groupBy("DISTRICT", "STREET", "OFFENSE_CODE")
                .count()
                .withColumn("ratio",
                        col("count").divide(callUDF("myUDF", col("DISTRICT"))).multiply(100));

        aggregation.show();
    }
}
