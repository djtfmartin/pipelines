package org.example;

import org.apache.spark.sql.*;
        import org.apache.spark.sql.types.*;
        import java.util.*;
        import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SerializeWithRow {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Parquet Row Example")
                .master("local[*]")
                .getOrCreate();

        System.out.println("🚀 Spark Session started.");
        long startTime = System.currentTimeMillis();

        // Define schema manually (6 string fields)
        StructType schema = new StructType(new StructField[]{
                new StructField("field1", DataTypes.StringType, false, Metadata.empty()),
                new StructField("field2", DataTypes.StringType, false, Metadata.empty()),
                new StructField("field3", DataTypes.StringType, false, Metadata.empty()),
                new StructField("field4", DataTypes.StringType, false, Metadata.empty()),
                new StructField("field5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("field6", DataTypes.StringType, false, Metadata.empty())
        });

        // Generate 100,000 Rows with nonsense data
        int amount = 3_000_000;
        List<Row> rows = IntStream.range(0, amount)
                .mapToObj(i -> RowFactory.create(
                        randomString(), randomString(), randomString(),
                        randomString(), randomString(), randomString()))
                .collect(Collectors.toList());

        // Create DataFrame (Dataset<Row>)
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        // Write to Parquet
        String outputPath = "file:///tmp/output/parquet_rows";
        df.write().mode(SaveMode.Overwrite).parquet(outputPath);

        System.out.println("✅ Successfully wrote "+amount+ " Rows to Parquet at: " + outputPath);

        spark.stop();

        long endTime = System.currentTimeMillis();
        System.out.println("⏱️ Execution time: " + (endTime - startTime) + " ms");
    }

    // Utility to make random nonsense strings
    private static String randomString() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
