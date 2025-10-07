/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
import scala.Tuple2;

@Slf4j
public class InterpretationJoin implements Serializable {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = "--coreSiteConfig", description = "Path to core-site.xml", required = false)
    private String coreSiteConfig;

    @Parameter(names = "--hdfsSiteConfig", description = "Path to hdfs-site.xml", required = false)
    private String hdfsSiteConfig;

    @Parameter(names = "--properties", description = "Path to properties file", required = true)
    private String properties;

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(names = "--debugOutput", description = "Debug output", required = false, arity = 1)
    private boolean debug = false;

    @Parameter(names = "--hdfsView", description = "Debug output", required = false, arity = 1)
    private boolean hdfsView = true;

    @Parameter(names = "--jsonView", description = "Debug output", required = false, arity = 1)
    private boolean jsonView = true;

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    private int numberOfShards = 10;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws Exception {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.properties);

    String datasetId = args.datasetId;
    int attempt = args.attempt;
    String inputPath = String.format("%s/%s/%d", config.getInputPath(), datasetId, attempt);
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(args.appName);
    if (args.master != null && !args.master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(args.master);
    }

    String warehouseDirectory =
        "hdfs://gbif-hdfs/data/ingest_spark/" + datasetId + "/" + attempt + "/dave-swarehouse2";

    sparkBuilder.config("spark.sql.warehouse.dir", warehouseDirectory);
    SparkSession spark = sparkBuilder.getOrCreate();

    //    spark
    //        .sparkContext()
    //        .setJobGroup("load-avro", String.format("Load extended records from %s", inputPath),
    // true);
    //    Dataset<ExtendedRecord> extendedRecords =
    //        loadExtendedRecords(spark, inputPath, args.numberOfShards);

    //    log.info("=== Step 1: Initialise occurrence records {}", extendedRecords.count());

    spark
        .sparkContext()
        .setJobGroup("initialise-occurrence", "Initialise occurrence records", true);

    log.info("=== Step 2: Load metadata from registry and ES");
    spark.sparkContext().setJobGroup("load-metadata", "Load metadata from registry and ES", true);

    spark.sql("DROP TABLE IF EXISTS identifier");
    spark.sparkContext().setJobGroup("load", "Loading identifiers into warehouse", true);
    loadIdentifersIntoWarehouse(spark, outputPath);

    spark.sparkContext().setJobGroup("load", "Loading verbatim into warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "verbatim", args.numberOfShards);

    spark.sparkContext().setJobGroup("load", "Loading basic into warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "basic", args.numberOfShards);

    spark.sparkContext().setJobGroup("load", "Loading temporalinto warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "temporal", args.numberOfShards);

    spark.sparkContext().setJobGroup("load", "Loading taxonomy into warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "taxonomy", args.numberOfShards);

    spark.sparkContext().setJobGroup("load", "Loading grscicoll into warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "grscicoll", args.numberOfShards);

    spark.sparkContext().setJobGroup("load", "Loading location into warehouse", true);
    loadRecordTypeAsRow(spark, outputPath, "location", args.numberOfShards);

    // --- The JOIN -- //

    // reload from disk....
    spark.read().parquet(warehouseDirectory + "/basic").createOrReplaceTempView("basic");
    spark.read().parquet(warehouseDirectory + "/grscicoll").createOrReplaceTempView("grscicoll");
    spark.read().parquet(warehouseDirectory + "/location").createOrReplaceTempView("location");
    spark.read().parquet(warehouseDirectory + "/taxonomy").createOrReplaceTempView("taxonomy");
    spark.read().parquet(warehouseDirectory + "/temporal").createOrReplaceTempView("temporal");
    spark.read().parquet(warehouseDirectory + "/verbatim").createOrReplaceTempView("verbatim");

    spark.sparkContext().setJobGroup("join", "Joining all together", true);
    spark
        .sql(
            "SELECT v.id, v.verbatim as verbatim, i.identifier as identifier, b.basic as basic, t.temporal as temporal, ta.taxonomy as taxonomy, g.grscicoll as grscicoll, l.location as location "
                + "FROM verbatim v "
                + "LEFT JOIN identifier i  ON v.id = i.id "
                + "LEFT JOIN basic       b  ON v.id = b.id "
                + "LEFT JOIN temporal    t  ON v.id = t.id "
                + "LEFT JOIN taxonomy    ta ON v.id = ta.id "
                + "LEFT JOIN grscicoll   g  ON v.id = g.id "
                + "LEFT JOIN location    l  ON v.id = l.id ")
        .write()
        .format("parquet")
        .mode("overwrite")
        .saveAsTable("joined");

    // --- HDFS VIEW -- //
    //
    //    // re-read the joined table
    //    MetadataRecord metadata = getMetadataRecord(config, args);
    //
    //    // output hdfs
    //    spark.sparkContext().setJobGroup("join", "Output hdfs", true);
    //    transformJsonToHdfsView(joined, metadata)
    //        .write()
    //        .mode("overwrite")
    //        .parquet(outputPath + "/hdfs-join");
    //    System.out.println("Wrote to: " + outputPath + "/hdfs-join");
    //
    //
    //      //--- JSON VIEW -- //
    //
    //
    //    // output json
    //    spark.sparkContext().setJobGroup("join", "Output json", true);
    //    transformToJsonView(joined, metadata).write().mode("overwrite").parquet(outputPath +
    // "/json");
    //    System.out.println("Wrote to: " + outputPath + "/json-join");

    spark.close();
    System.exit(0);
  }

  private static void loadIdentifersIntoWarehouse(SparkSession spark, String outputPath) {
    ObjectMapper MAPPER = new ObjectMapper();
    loadIdentifiers(spark, outputPath)
        .map(
            (MapFunction<IdentifierRecord, Tuple2<String, String>>)
                record ->
                    Tuple2.<String, String>apply(record.getId(), MAPPER.writeValueAsString(record)),
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF("id", "identifier")
        .write()
        .format("parquet")
        .mode("overwrite")
        .saveAsTable("identifier");
  }

  private static MetadataRecord getMetadataRecord(PipelinesConfig config, Args args) {
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);
    return metadata;
  }
  // ----------------- Helper Methods -----------------

  private static Dataset<ExtendedRecord> loadExtendedRecords(
      SparkSession spark, String inputPath, int numberOfShards) {
    return spark
        .read()
        .format("avro")
        .load(inputPath + "/verbatim.avro")
        .as(Encoders.bean(ExtendedRecord.class))
        .repartition(numberOfShards);
  }

  private static Dataset<Tuple2<String, String>> loadRecordType(
      SparkSession spark, String outputPath, String recordType) {
    return spark
        .read()
        .parquet(outputPath + "/" + recordType)
        .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
  }

  private static void loadRecordTypeAsRow(
      SparkSession spark, String outputPath, String recordType, int numberOfShards) {

    Dataset<Row> loadedFromFiles =
        spark
            .read()
            .parquet(outputPath + "/" + recordType)
            .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
            .toDF("id", recordType);

    // write sorted and bucketed table
    spark.sql("DROP TABLE IF EXISTS " + recordType);
    loadedFromFiles
        .repartition(numberOfShards)
        .write()
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(recordType);
  }

  private static Dataset<IdentifierRecord> loadIdentifiers(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/identifiers")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  private static void writeDebug(
      SparkSession spark,
      Dataset<Tuple2<String, String>> records,
      String outputPath,
      String name,
      boolean debug) {

    if (debug) {
      log.info("Writing debug {}", name);
      spark
          .sparkContext()
          .setJobGroup(
              String.format("write-%s", name), String.format("Write %s to Parquet", name), true);
      records.write().mode("overwrite").parquet(outputPath + "/" + name);
    }
  }

  static final ObjectMapper objectMapper = new ObjectMapper();

  private static Dataset<Tuple2<String, String>> basicTransform(
      PipelinesConfig config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, Tuple2<String, String>>)
            er -> {
              return Tuple2.apply(
                  er.getId(),
                  objectMapper.writeValueAsString(
                      BasicTransform.builder().config(config).build().convert(er).get()));
            },
        Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
  }

  private static Dataset<Row> joinAsRowTo(
      Dataset<Row> source,
      Dataset<Tuple2<String, String>> records,
      String targetColumn,
      String outputPath) {

    // Perform join and add the joined value into a new column
    Dataset<Row> joinedDataset =
        source
            .join(records, source.col("id").equalTo(records.col("_1")), "inner")
            .drop(records.col("_1"))
            .withColumnRenamed(records.col("_2").toString(), targetColumn);

    joinedDataset.write().mode("overwrite").parquet(outputPath + "/joined-" + targetColumn);

    return joinedDataset;
  }
}
