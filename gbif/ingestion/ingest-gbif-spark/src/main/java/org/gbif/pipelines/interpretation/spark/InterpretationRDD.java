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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import scala.*;

@Slf4j
public class InterpretationRDD implements Serializable {

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

  public static void main(String[] argsv) {

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
    SparkSession spark = sparkBuilder.getOrCreate();

    spark
        .sparkContext()
        .setJobGroup("load-avro", String.format("Load extended records from %s", inputPath), true);
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, inputPath, args.numberOfShards);

    log.info("=== Step 1: Initialise occurrence records {}", extendedRecords.count());

    spark
        .sparkContext()
        .setJobGroup("initialise-occurrence", "Initialise occurrence records", true);

    log.info("=== Step 2: Load metadata from registry and ES");
    spark.sparkContext().setJobGroup("load-metadata", "Load metadata from registry and ES", true);
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    //    log.info("=== Step 5: Interpret location");
    //    spark.sparkContext().setJobGroup("location-transform", "Run location transform", true);
    //    Dataset<Tuple2<String, String>> location =
    //        locationTransform(
    //            config, spark, extendedRecords, metadata, args.numberOfShards, outputPath);
    //
    //    log.info("Count of looked up locations {}", location.count());
    //    writeDebug(spark, location, outputPath, "location", args.debug);

    //
    //    log.info("=== Step 4: Interpret basic terms");
    //    spark.sparkContext().setJobGroup("basic-transform", "Run basic transform", true);
    //    Dataset<Tuple2<String, String>> basic = basicTransform(config, extendedRecords);
    //    writeDebug(spark, basic, outputPath, "basic", args.debug);
    //
    //    log.info("=== Step 6: Interpret temporal");
    //    spark.sparkContext().setJobGroup("temporal-transform", "Run temporal transform", true);
    //    Dataset<Tuple2<String, String>> temporal = temporalTransform(extendedRecords);
    //    writeDebug(spark, temporal, outputPath, "temporal", args.debug);
    //
    //    log.info("=== Step 7: Interpret taxonomy");
    //    spark.sparkContext().setJobGroup("taxonomy-transform", "Run taxonomy transform", true);
    //    Dataset<Tuple2<String, String>> multiTaxon =
    //        taxonomyTransform(config, spark, extendedRecords, args.numberOfShards, outputPath);
    //    writeDebug(spark, multiTaxon, outputPath, "taxonomy", args.debug);
    //
    //    log.info("=== Step 8: Interpret GrSciColl");
    //    spark.sparkContext().setJobGroup("grscicoll-transform", "Run grscicoll transform", true);
    //    Dataset<Tuple2<String, String>> grscicoll =
    //        grscicollTransform(config, spark, extendedRecords, metadata, args.numberOfShards);
    //
    //      log.info("=== Step 3: Load identifiers from {}", outputPath);
    //      spark
    //              .sparkContext()
    //              .setJobGroup(
    //                      "load-identifiers", String.format("Load extended records from %s",
    // outputPath), true);
    //      Dataset<Tuple2<String, String>> identifiers =
    //              loadIdentifiers(spark, outputPath)
    //                      .map(
    //                              (MapFunction<IdentifierRecord, Tuple2<String, String>>)
    //                                      ir -> {
    //                                          return Tuple2.apply(ir.getId(),
    // objectMapper.writeValueAsString(ir));
    //                                      },
    //                              Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
    //
    //

    ObjectMapper MAPPER = new ObjectMapper();

    JavaPairRDD<String, String> occurrenceRecords =
        extendedRecords
            .map(
                (MapFunction<ExtendedRecord, Tuple2<String, String>>)
                    record ->
                        Tuple2.<String, String>apply(
                            record.getId(), MAPPER.writeValueAsString(record)),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
            .toDF("id", "json")
            .javaRDD()
            .mapToPair(
                new PairFunction<Row, String, String>() {
                  @Override
                  public Tuple2<String, String> call(Row row) throws Exception {
                    return Tuple2.<String, String>apply(row.getAs("id"), row.getAs("json"));
                  }
                });
    log.info("Identifiers count {}", occurrenceRecords.count());
    JavaPairRDD<String, String> identifiersRDD =
        loadIdentifiers(spark, outputPath)
            .map(
                (MapFunction<IdentifierRecord, Tuple2<String, String>>)
                    record ->
                        Tuple2.<String, String>apply(
                            record.getId(), MAPPER.writeValueAsString(record)),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
            .toDF("id", "json")
            .javaRDD()
            .mapToPair(
                new PairFunction<Row, String, String>() {
                  @Override
                  public Tuple2<String, String> call(Row row) throws Exception {
                    return Tuple2.<String, String>apply(row.getAs("id"), row.getAs("json"));
                  }
                });

    spark.sparkContext().setJobGroup("initialise-occurrence", "Loading preprepared parquet", true);

    JavaPairRDD<String, String> basicRDD = loadRecordTypeAsJDD(spark, outputPath, "basic");
    System.out.println("JSON count: " + basicRDD.count());
    JavaPairRDD<String, String> temporalRDD = loadRecordTypeAsJDD(spark, outputPath, "temporal");
    System.out.println("JSON count: " + temporalRDD.count());
    JavaPairRDD<String, String> taxonomyRDD = loadRecordTypeAsJDD(spark, outputPath, "taxonomy");
    System.out.println("JSON count: " + taxonomyRDD.count());
    JavaPairRDD<String, String> grscicollRDD = loadRecordTypeAsJDD(spark, outputPath, "grscicoll");
    System.out.println("JSON count: " + grscicollRDD.count());
    JavaPairRDD<String, String> locationRDD = loadRecordTypeAsJDD(spark, outputPath, "location");
    System.out.println("JSON count: " + locationRDD.count());

    spark.sparkContext().setJobGroup("initialise-occurrence", "cogroup 1", true);

    JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cg1 =
        occurrenceRecords.cogroup(identifiersRDD, basicRDD);

    System.out.println("Cogroup 1 count: " + cg1.count());

    spark.sparkContext().setJobGroup("initialise-occurrence", "cogroup 2", true);

    // Step 2: Cogroup the next 3 with the result
    JavaPairRDD<
            String,
            Tuple4<
                Iterable<Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>,
                Iterable<String>,
                Iterable<String>,
                Iterable<String>>>
        cg2 = cg1.cogroup(temporalRDD, taxonomyRDD, grscicollRDD);

    System.out.println("Cogroup 2 count: " + cg2.count());

    spark.sparkContext().setJobGroup("initialise-occurrence", "cogroup 3", true);

    // Step 3: Cogroup the last one
    JavaPairRDD<
            String,
            Tuple2<
                Iterable<
                    Tuple4<
                        Iterable<Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>,
                        Iterable<String>,
                        Iterable<String>,
                        Iterable<String>>>,
                Iterable<String>>>
        finalCg = cg2.cogroup(locationRDD);

    System.out.println("Cogroup 3 count: " + finalCg.count());

    spark.sparkContext().setJobGroup("json", "map to json", true);

    JavaRDD<OccurrenceJsonRecord> json =
        finalCg.map(
            data -> {
              String key = data._1;
              Tuple2<
                      Iterable<
                          Tuple4<
                              Iterable<
                                  Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>,
                              Iterable<String>,
                              Iterable<String>,
                              Iterable<String>>>,
                      Iterable<String>>
                  obj = data._2;

              Tuple4<
                      Iterable<Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>,
                      Iterable<String>,
                      Iterable<String>,
                      Iterable<String>>
                  t = obj._1().iterator().next();
              System.out.println("Key: " + key);
              Tuple3<Iterable<String>, Iterable<String>, Iterable<String>> t3 =
                  t._1().iterator().next();
              //                  System.out.println("  Extended: " + t3._1());
              //                  System.out.println("  Identifier: " + t3._2());
              //                  System.out.println("  Basic: " + t3._3());
              //                  System.out.println("  Temporal: " + t._2());
              //                  System.out.println("  Taxonomy: " + t._3());
              //                  System.out.println("  Griscoll: " + t._4());
              //                  System.out.println("  Location: " + obj._2());
              //
              ExtendedRecord verbatim =
                  MAPPER.readValue(t3._1().iterator().next(), ExtendedRecord.class);
              IdentifierRecord identifierRecord =
                  MAPPER.readValue(t3._2().iterator().next(), IdentifierRecord.class);
              BasicRecord basic = MAPPER.readValue(t3._3().iterator().next(), BasicRecord.class);
              LocationRecord location =
                  MAPPER.readValue(obj._2().iterator().next(), LocationRecord.class);
              TemporalRecord temporalRecord =
                  MAPPER.readValue(t._2().iterator().next(), TemporalRecord.class);
              MultiTaxonRecord multiTaxonRecord =
                  MAPPER.readValue(t._3().iterator().next(), MultiTaxonRecord.class);
              GrscicollRecord grscicollRecord =
                  MAPPER.readValue(t._4().iterator().next(), GrscicollRecord.class);

              return OccurrenceJsonConverter.builder()
                  .verbatim(verbatim)
                  .basic(basic)
                  .location(location)
                  .temporal(temporalRecord)
                  .multiTaxon(multiTaxonRecord)
                  .grscicoll(grscicollRecord)
                  .identifier(identifierRecord)
                  .metadata(metadata)
                  .clustering(ClusteringRecord.newBuilder().setId(key).build()) // placeholder
                  .multimedia(MultimediaRecord.newBuilder().setId(key).build()) // placeholder
                  .build()
                  .convert();
            });

    // output the json RDD to parquet
    System.out.println("JSON count: " + json.count());

    spark.sparkContext().setJobGroup("json", "write json to parquet", true);
    Dataset<OccurrenceJsonRecord> jsonDF =
        spark.createDataset(json.rdd(), Encoders.bean(OccurrenceJsonRecord.class));
    jsonDF.write().mode("overwrite").parquet(outputPath + "/json-cogroup");

    //      JavaPairRDD<String, Tuple2<Iterable<String>, Tuple2<Iterable<String>,
    // Iterable<String>>>> cogrouped =
    //      JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>
    // cogrouped =
    //              occurrenceRecords.cogroup(
    //                      identifiers,
    //                      basic
    //              );

    //      JavaPairRDD<String, Tuple4<Iterable<String>, Iterable<String>, Iterable<String>,
    // Iterable<String>>> cogrouped =
    //              (occurrenceRecords).groupWith(
    //                      identifiers,
    //                      basic,
    //                      location
    //              );

    //
    //      // Flatten cogroup results: (key, val1, val2, val3)
    //      JavaRDD<OccurrenceHdfsRecord> occurrenceHdfsRdd = cogrouped.map(data -> {
    //          String key = data._1;
    //          System.out.println("  key: " + key);
    //
    //          String extendedRecord = data._2._1().iterator().next();
    //          String identifierRecord = data._2._2().iterator().next();
    //          String basicRecord = data._2._3().iterator().next();
    //
    //          return OccurrenceHdfsRecordConverter.builder()
    //                  .metadataRecord(metadata)
    //                  .extendedRecord(MAPPER.readValue(extendedRecord, ExtendedRecord.class))
    //                  .identifierRecord(MAPPER.readValue(identifierRecord,
    // IdentifierRecord.class))
    //                  .basicRecord(MAPPER.readValue(basicRecord, BasicRecord.class))
    //                  .locationRecord(MAPPER.readValue((String) record.getAs("location"),
    // LocationRecord.class))
    //                  .temporalRecord(MAPPER.readValue((String) record.getAs("temporal"),
    // TemporalRecord.class))
    //                  .multiTaxonRecord(MAPPER.readValue((String) record.getAs("taxonomy"),
    // MultiTaxonRecord.class))
    //                  .grscicollRecord(MAPPER.readValue((String) record.getAs("grscicoll"),
    // GrscicollRecord.class))
    //                  .build()
    //                  .convert();
    //
    //      });

    //      // Convert RDD to Dataset<Row> using reflection
    //      Dataset<Row> peopleDF = spark.createDataFrame(occurrenceHdfsRdd,
    // OccurrenceHdfsRecord.class);

    // Write to Parquet
    //      peopleDF.write()
    //              .mode(SaveMode.Overwrite) // Overwrite existing files
    //              .parquet("output/people.parquet");

    //
    //    spark.sparkContext().setJobGroup("join-identifiers", "Join identifiers to occurrence",
    // true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, identifiers, "identifier", outputPath);
    //    log.info("Join 1 count {}", occurrenceRecords.count());
    //
    //    Dataset<Tuple2<String, String>> basic = loadRecordType(spark, outputPath, "basic");
    //    spark.sparkContext().setJobGroup("join-basic", "Join basic to occurrence", true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, basic, "basic", outputPath);
    //    log.info("Join 2 count {}", occurrenceRecords.count());
    //
    //    Dataset<Tuple2<String, String>> location = loadRecordType(spark, outputPath, "location");
    //    spark.sparkContext().setJobGroup("join-location", "Join location to occurrence", true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, location, "location", outputPath);
    //    log.info("Join 3 count {}", occurrenceRecords.count());
    //
    //    Dataset<Tuple2<String, String>> temporal = loadRecordType(spark, outputPath, "temporal");
    //    spark.sparkContext().setJobGroup("join-temporal", "Join temporal to occurrence", true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, temporal, "temporal", outputPath);
    //    log.info("Join 4 count {}", occurrenceRecords.count());
    //
    //    Dataset<Tuple2<String, String>> taxonomy = loadRecordType(spark, outputPath, "taxonomy");
    //    spark.sparkContext().setJobGroup("join-multitaxon", "Join multitaxon to occurrence",
    // true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, taxonomy, "taxonomy", outputPath);
    //    log.info("Join 5 count {}", occurrenceRecords.count());
    //
    //    Dataset<Tuple2<String, String>> grscicoll = loadRecordType(spark, outputPath,
    // "grscicoll");
    //    spark.sparkContext().setJobGroup("join-grscicoll", "Join grscicoll to occurrence", true);
    //    occurrenceRecords = joinAsRowTo(occurrenceRecords, grscicoll, "grscicoll", outputPath);
    //    log.info("Join 6 count {}", occurrenceRecords.count());

    //    if (args.hdfsView) {
    //      log.info("=== Step 9: Generate HDFS view");
    //      spark.sparkContext().setJobGroup("hdfs-view", "Generate HDFS view", true);
    //      Dataset<OccurrenceHdfsRecord> hdfsView = transformJsonToHdfsView(occurrenceRecords,
    // metadata);
    //      log.info("HDFS count {}", occurrenceRecords.count());
    //      hdfsView.write().mode("overwrite").parquet(outputPath + "/hdfsview");
    //    }

    //    //
    //    //    if (args.jsonView) {
    //    //      log.info("=== Step 10: Generate JSON view");
    //    //      spark.sparkContext().setJobGroup("json-view", "Generate JSON view", true);
    //    //      Dataset<OccurrenceJsonRecord> jsonView = transformToJsonView(occurrenceRecords,
    //    // metadata);
    //    //      jsonView.write().mode("overwrite").parquet(outputPath + "/json");
    //    //    }
    //
    //    log.info(
    //        "=== Interpretation pipeline finished successfully in {} seconds ===",
    //        (System.currentTimeMillis() - spark.sparkContext().startTime()) / 1000);
    ;
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

  private static JavaPairRDD<String, String> loadRecordTypeAsJDD(
      SparkSession spark, String outputPath, String recordType) {
    return spark
        .read()
        .parquet(outputPath + "/" + recordType)
        .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF("id", "json")
        .javaRDD()
        .mapToPair(
            new PairFunction<Row, String, String>() {
              @Override
              public Tuple2<String, String> call(Row row) throws Exception {
                return Tuple2.<String, String>apply(row.getAs("id"), row.getAs("json"));
              }
            });
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
