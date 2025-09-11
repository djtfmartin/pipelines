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

import static org.gbif.dwc.terms.DwcTerm.parentEventID;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.GrscicollInterpretation.grscicollTransform;
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformJsonToHdfsView;
import static org.gbif.pipelines.interpretation.spark.JsonView.transformToJsonView;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.Serializable;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import scala.Tuple2;

@Slf4j
public class Interpretation implements Serializable {

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

    spark
        .sparkContext()
        .setJobGroup("initialise-occurrence", "Initialise occurrence records", true);


    log.info("=== Step 2: Load metadata from registry and ES");
    spark.sparkContext().setJobGroup("load-metadata", "Load metadata from registry and ES", true);
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    log.info("=== Step 3: Load identifiers from {}", outputPath);
    spark
        .sparkContext()
        .setJobGroup(
            "load-identifiers", String.format("Load extended records from %s", outputPath), true);
    Dataset<Tuple2<String, String>> identifiers =  loadIdentifiers(spark, outputPath).map((MapFunction<IdentifierRecord, Tuple2<String, String>>) ir -> {
          return Tuple2.apply(ir.getId(), objectMapper.writeValueAsString(ir));
      }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

    log.info("=== Step 4: Interpret basic terms");
    spark.sparkContext().setJobGroup("basic-transform", "Run basic transform", true);
    Dataset<Tuple2<String, String>> basic = basicTransform(config, extendedRecords);
//    writeDebug(spark, basic, outputPath, "basic", args.debug);

    log.info("=== Step 5: Interpret location");
    spark.sparkContext().setJobGroup("location-transform", "Run location transform", true);
    Dataset<Tuple2<String, String>> location =
        locationTransform(config, spark, extendedRecords, metadata, args.numberOfShards);
//    writeDebug(spark, location, outputPath, "location", args.debug);

    log.info("=== Step 6: Interpret temporal");
    spark.sparkContext().setJobGroup("temporal-transform", "Run temporal transform", true);
    Dataset<Tuple2<String, String>> temporal = temporalTransform(extendedRecords);
//    writeDebug(spark, temporal, outputPath, "temporal", args.debug);

    log.info("=== Step 7: Interpret taxonomy");
    spark.sparkContext().setJobGroup("taxonomy-transform", "Run taxonomy transform", true);
    Dataset<Tuple2<String, String>> multiTaxon =
        taxonomyTransform(config, spark, extendedRecords, args.numberOfShards);
//    writeDebug(spark, multiTaxon, outputPath, "taxonomy", args.debug);

    log.info("=== Step 8: Interpret GrSciColl");
    spark.sparkContext().setJobGroup("grscicoll-transform", "Run grscicoll transform", true);
    Dataset<Tuple2<String,String>> grscicoll =
        grscicollTransform(config, spark, extendedRecords, metadata, args.numberOfShards);
//    writeDebug(spark, grscicoll, outputPath, "grscicoll", args.debug);

//    // Join all interpreted datasets into occurrence
//    spark.sparkContext().setJobGroup("join-identifiers", "Join identifiers to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, identifiers, OccurrenceRecord::setIdentifier);
//    spark.sparkContext().setJobGroup("join-basic", "Join basic to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, basic, OccurrenceRecord::setBasic);
//    spark.sparkContext().setJobGroup("join-location", "Join location to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, location, OccurrenceRecord::setLocation);
//    spark.sparkContext().setJobGroup("join-temporal", "Join temporal to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, temporal, OccurrenceRecord::setTemporal);
//    spark.sparkContext().setJobGroup("join-multitaxon", "Join multitaxon to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, multiTaxon, OccurrenceRecord::setMultiTaxon);
//    spark.sparkContext().setJobGroup("join-grscicoll", "Join grscicoll to occurrence", true);
//    occurrenceRecords = joinTo(occurrenceRecords, grscicoll, OccurrenceRecord::setGrscicoll);

  Dataset<OccurrenceRecordJSON> occurrenceRecords =
          extendedRecords.map(
                  new ExtendedToOccurrenceJSONMapper(), Encoders.bean(OccurrenceRecordJSON.class));

    spark.sparkContext().setJobGroup("join-identifiers", "Join identifiers to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, identifiers, OccurrenceRecordJSON::setIdentifier);
    spark.sparkContext().setJobGroup("join-basic", "Join basic to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, basic, OccurrenceRecordJSON::setBasic);
    spark.sparkContext().setJobGroup("join-location", "Join location to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, location, OccurrenceRecordJSON::setLocation);
    spark.sparkContext().setJobGroup("join-temporal", "Join temporal to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, temporal, OccurrenceRecordJSON::setTemporal);
    spark.sparkContext().setJobGroup("join-multitaxon", "Join multitaxon to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, multiTaxon, OccurrenceRecordJSON::setMultiTaxon);
    spark.sparkContext().setJobGroup("join-grscicoll", "Join grscicoll to occurrence", true);
    occurrenceRecords = joinAsJsonTo(occurrenceRecords, grscicoll, OccurrenceRecordJSON::setGrscicoll);

    if (args.hdfsView) {
      log.info("=== Step 9: Generate HDFS view");
      spark.sparkContext().setJobGroup("hdfs-view", "Generate HDFS view", true);
      Dataset<OccurrenceHdfsRecord> hdfsView = transformJsonToHdfsView(occurrenceRecords, metadata);
      hdfsView.write().mode("overwrite").parquet(outputPath + "/hdfsview");
    }

    if (args.jsonView) {
      log.info("=== Step 10: Generate JSON view");
      spark.sparkContext().setJobGroup("json-view", "Generate JSON view", true);
      Dataset<OccurrenceJsonRecord> jsonView = transformToJsonView(occurrenceRecords, metadata);
      jsonView.write().mode("overwrite").parquet(outputPath + "/json");
    }

    log.info(
        "=== Interpretation pipeline finished successfully in {} seconds ===",
        (System.currentTimeMillis() - spark.sparkContext().startTime()) / 1000);
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

  private static Dataset<IdentifierRecord> loadIdentifiers(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/identifiers")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  private static <T> void writeDebug(
      SparkSession spark,
      Dataset<? extends Record> records,
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

  // Mapper with explicit name for Spark UI
  private static class ExtendedToOccurrenceMapper
      implements MapFunction<ExtendedRecord, OccurrenceRecord> {
    @Override
    public OccurrenceRecord call(ExtendedRecord extendedRecord) {
      return OccurrenceRecord.builder()
          .verbatim(extendedRecord)
          .id(extendedRecord.getId())
          .coreId(extendedRecord.getCoreId())
          .parentId(extractValue(extendedRecord, parentEventID))
          .build();
    }
  }

  final static ObjectMapper objectMapper = new ObjectMapper();

    // Mapper with explicit name for Spark UI
    private static class ExtendedToOccurrenceJSONMapper
            implements MapFunction<ExtendedRecord, OccurrenceRecordJSON> {
        @Override
        public OccurrenceRecordJSON call(ExtendedRecord extendedRecord) {
            try {
                return OccurrenceRecordJSON.builder()
                        .verbatim(objectMapper.writeValueAsString(extendedRecord))
                        .id(extendedRecord.getId())
                        .coreId(extendedRecord.getCoreId())
                        .parentId(extractValue(extendedRecord, parentEventID))
                        .build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

  private static Dataset<Tuple2<String, String>> basicTransform(
      PipelinesConfig config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, Tuple2<String, String>>)
            er -> {
                return Tuple2.apply(er.getId(), objectMapper.writeValueAsString(BasicTransform.builder()
                        .useDynamicPropertiesInterpretation(true)
                        .vocabularyApiUrl(config.getVocabularyService().getWsUrl())
                        .build()
                        .convert(er)
                        .get()));
            }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
  }

  @FunctionalInterface
  public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {}

  private static <R extends Record> Dataset<OccurrenceRecord> joinTo(
      Dataset<OccurrenceRecord> source,
      Dataset<R> records,
      SerializableBiConsumer<OccurrenceRecord, R> recordMapper) {
    return source
        .joinWith(records, source.col("id").equalTo(records.col("id")))
        .map(
            (MapFunction<Tuple2<OccurrenceRecord, R>, OccurrenceRecord>)
                row -> {
                  OccurrenceRecord r = row._1;
                  recordMapper.accept(r, row._2);
                  return r;
                },
            Encoders.bean(OccurrenceRecord.class));
  }

    private static <R extends Record> Dataset<OccurrenceRecordJSON> joinAsJsonTo(
            Dataset<OccurrenceRecordJSON> source,
            Dataset<Tuple2<String, String>> records,
            SerializableBiConsumer<OccurrenceRecordJSON, String> recordMapper) {

        return source
                .joinWith(records, source.col("id").equalTo(records.col("_1")))
                .map(
                        (MapFunction<Tuple2<OccurrenceRecordJSON, Tuple2<String, String>>, OccurrenceRecordJSON>)
                                row -> {
                                    OccurrenceRecordJSON record = row._1;
                                    recordMapper.accept(record, row._2._2);
                                    return record;
                                },
                        Encoders.bean(OccurrenceRecordJSON.class));
    }
}
