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
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformJsonToHdfsView;
import static org.gbif.pipelines.interpretation.spark.JsonView.transformToJsonView;

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
public class InterpretationJoinWithBucketing implements Serializable {

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

  static final int BUCKETS = 25;

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

    sparkBuilder.config(
        "spark.sql.warehouse.dir",
        "hdfs://gbif-hdfs/data/ingest_spark/" + datasetId + "/" + attempt + "/dave-warehouse");

    sparkBuilder.config("hive.exec.dynamic.partition", "true");
    sparkBuilder.config("hive.exec.dynamic.partition.mode", "nonstrict");
    sparkBuilder.config("hive.exec.max.dynamic.partitions", "300");
    sparkBuilder.config("hive.exec.max.dynamic.partitions.pernode", "400");
    sparkBuilder.config("hive.enforce.bucketing", "true");
    sparkBuilder.config("optimize.sort.dynamic.partitionining", "true");
    sparkBuilder.config("hive.vectorized.execution.enabled", "true");
    sparkBuilder.config("hive.enforce.sorting", "true");

    SparkSession spark = sparkBuilder.getOrCreate();

    spark
        .sparkContext()
        .setJobGroup("initialise-occurrence", "Initialise occurrence records", true);

    log.info("=== Step 2: Load metadata from registry and ES");
    spark.sparkContext().setJobGroup("load-metadata", "Load metadata from registry and ES", true);
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    ObjectMapper MAPPER = new ObjectMapper();
    //
    //    spark.sql("DROP TABLE IF EXISTS verbatim");
    //    extendedRecords
    //        .map(
    //            (MapFunction<ExtendedRecord, Tuple2<String, String>>)
    //                record ->
    //                    Tuple2.<String, String>apply(record.getId(),
    // MAPPER.writeValueAsString(record)),
    //            Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
    //        .toDF("id", "verbatim")
    //        .write()
    //        .format("parquet")
    //        .bucketBy(BUCKETS, "id")
    //        .sortBy("id")
    //        .mode("overwrite")
    //        .saveAsTable("verbatim");
    //
    //    Dataset<Row> occurrenceRecords = spark.table("verbatim");

    spark.sql("DROP TABLE IF EXISTS identifier");
    loadIdentifiers(spark, outputPath)
        .map(
            (MapFunction<IdentifierRecord, Tuple2<String, String>>)
                record ->
                    Tuple2.<String, String>apply(record.getId(), MAPPER.writeValueAsString(record)),
            Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF("id", "identifier")
        .write()
        .bucketBy(BUCKETS, "id")
        .sortBy("id")
        .mode("overwrite")
        .saveAsTable("identifier");

    Dataset<Row> identifiersRDD = spark.table("identifier");
    System.out.println("JSON count: " + identifiersRDD.count());

    spark.sparkContext().setJobGroup("initialise-occurrence", "Loading pre-prepared parquet", true);

    spark.sparkContext().setJobGroup("load", "Loading verbatim", true);
    Dataset<Row> verbatimRDD = loadRecordTypeWithBucketing(spark, outputPath, "verbatim", BUCKETS);
    System.out.println("JSON count: " + verbatimRDD.count());

    spark.sparkContext().setJobGroup("load", "Loading basic", true);
    Dataset<Row> basicRDD = loadRecordTypeWithBucketing(spark, outputPath, "basic", BUCKETS);
    System.out.println("JSON count: " + basicRDD.count());

    spark.sparkContext().setJobGroup("load", "Loading temporal", true);
    Dataset<Row> temporalRDD = loadRecordTypeWithBucketing(spark, outputPath, "temporal", BUCKETS);
    System.out.println("JSON count: " + temporalRDD.count());

    spark.sparkContext().setJobGroup("load", "Loading taxonomy", true);
    Dataset<Row> taxonomyRDD = loadRecordTypeWithBucketing(spark, outputPath, "taxonomy", BUCKETS);
    System.out.println("JSON count: " + taxonomyRDD.count());

    spark.sparkContext().setJobGroup("load", "Loading grscicoll", true);
    Dataset<Row> grscicollRDD =
        loadRecordTypeWithBucketing(spark, outputPath, "grscicoll", BUCKETS);
    System.out.println("JSON count: " + grscicollRDD.count());

    spark.sparkContext().setJobGroup("load", "Loading location", true);
    Dataset<Row> locationRDD = loadRecordTypeWithBucketing(spark, outputPath, "location", BUCKETS);
    System.out.println("JSON count: " + locationRDD.count());

    spark.sparkContext().setJobGroup("join", "Joining all together", true);
    Dataset<Row> joined =
        spark.sql(
            "SELECT v.id, v.verbatim as verbatim, i.identifier as identifier, b.basic as basic, t.temporal as temporal, ta.taxonomy as taxonomy, g.grscicoll as grscicoll, l.location as location "
                + "FROM verbatim v "
                + "LEFT JOIN identifier i  ON v.id = i.id "
                + "LEFT JOIN basic       b  ON v.id = b.id "
                + "LEFT JOIN temporal    t  ON v.id = t.id "
                + "LEFT JOIN taxonomy    ta ON v.id = ta.id "
                + "LEFT JOIN grscicoll   g  ON v.id = g.id "
                + "LEFT JOIN location    l  ON v.id = l.id ");

    System.out.println("Joined count: " + joined.count());

    // output hdfs
    spark.sparkContext().setJobGroup("join", "Output hdfs", true);
    transformJsonToHdfsView(joined, metadata)
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/hdfs-join");
    System.out.println("Wrote to: " + outputPath + "/hdfs-join");

    // output json
    spark.sparkContext().setJobGroup("join", "Output json", true);
    transformToJsonView(joined, metadata).write().mode("overwrite").parquet(outputPath + "/json");
    System.out.println("Wrote to: " + outputPath + "/json-join");

    spark.sql("DROP TABLE IF EXISTS verbatim");
    spark.sql("DROP TABLE IF EXISTS identifier");
    spark.sql("DROP TABLE IF EXISTS basic");
    spark.sql("DROP TABLE IF EXISTS temporal");
    spark.sql("DROP TABLE IF EXISTS taxonomy");
    spark.sql("DROP TABLE IF EXISTS grscicoll");
    spark.sql("DROP TABLE IF EXISTS location");

    spark.close();
    System.exit(0);
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

  private static Dataset<Row> loadRecordTypeWithBucketing(
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
        .coalesce(5)
        .write()
        .bucketBy(BUCKETS, "id")
        .sortBy("id")
        .mode("overwrite")
        .saveAsTable(recordType);

    // load table
    return spark.table(recordType);
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
                      BasicTransform.builder()
                          .useDynamicPropertiesInterpretation(true)
                          .vocabularyApiUrl(config.getVocabularyService().getWsUrl())
                          .build()
                          .convert(er)
                          .get()));
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
