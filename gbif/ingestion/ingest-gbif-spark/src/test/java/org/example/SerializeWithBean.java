package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SerializeWithBean {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Parquet POJO Example")
                .master("local[*]")
                .getOrCreate();

        System.out.println("🚀 Spark Session started.");
        long startTime = System.currentTimeMillis();

        // Generate 100,000 POJOs with random strings
        List<Record> records = IntStream.range(0, 3_000_000)
                .mapToObj(i -> new Record(
                        randomString(), randomString(),
                        randomString(), randomString(),
                        randomString(), randomString()))
                .collect(Collectors.toList());

        // Convert list to Dataset
        Dataset<Record> ds = spark.createDataset(records, Encoders.bean(Record.class));

        // Write to Parquet
        String outputPath = "file:///tmp/output/parquet_bean";
        ds.write().mode(SaveMode.Overwrite).parquet(outputPath);

        System.out.println("✅ Successfully wrote 3_000_000 records to Parquet at: " + outputPath);
        spark.stop();
        long endTime = System.currentTimeMillis();
        System.out.println("⏱️ Execution time: " + (endTime - startTime) + " ms");
    }

    private static String randomString() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
