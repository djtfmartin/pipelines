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
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.*;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import scala.Tuple2;

@Slf4j
public class Interpretation implements Serializable {

  static final ObjectMapper MAPPER = new ObjectMapper();

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

    // Load the extended records
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, inputPath, args.numberOfShards);

    // Load identifiers
    Dataset<IdentifierRecord> identifiers = loadIdentifiers(spark, outputPath);

    // join and write to disk
    extendedRecords
        .as("extendedRecord")
        .joinWith(identifiers, extendedRecords.col("id").equalTo(identifiers.col("id")))
        .map(
            (MapFunction<Tuple2<ExtendedRecord, IdentifierRecord>, OccurrenceSimple>)
                row -> {
                  ExtendedRecord er = row._1;
                  IdentifierRecord ir = row._2;
                  return OccurrenceSimple.builder()
                      .id(er.getId())
                      .verbatim(MAPPER.writeValueAsString(er))
                      .identifier(MAPPER.writeValueAsString(ir))
                      .build();
                },
            Encoders.bean(OccurrenceSimple.class))
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/extended-identifiers");

    // A single call to the registry to get the dataset metadata
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    final MetadataRecord metadata =
        MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    // Set up our transforms
    MultiTaxonomyTransform taxonomyTransform =
        MultiTaxonomyTransform.builder().config(config).build();
    LocationTransform locationTransform = LocationTransform.builder().config(config).build();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().config(config).build();
    TemporalTransform temporalTransform = TemporalTransform.builder().build();
    BasicTransform basicTransform = BasicTransform.builder().config(config).build();

    Dataset<OccurrenceSimple> simpleRecords =
        spark
            .read()
            .parquet(outputPath + "/extended-identifiers")
            .as(Encoders.bean(OccurrenceSimple.class));

    // Loop over all records and interpret them
    Dataset<OccurrenceSimple> interpreted =
        simpleRecords.map(
            (MapFunction<OccurrenceSimple, OccurrenceSimple>)
                simpleRecord -> {
                  ExtendedRecord er =
                      MAPPER.readValue(simpleRecord.getVerbatim(), ExtendedRecord.class);
                  // Apply all transforms
                  Optional<MultiTaxonRecord> tr = taxonomyTransform.convert(er);
                  Optional<LocationRecord> lr = locationTransform.convert(er, metadata);
                  Optional<GrscicollRecord> gr = grscicollTransform.convert(er, metadata);
                  Optional<TemporalRecord> ter = temporalTransform.convert(er);
                  Optional<BasicRecord> br = basicTransform.convert(er);
                  return OccurrenceSimple.builder()
                      .id(er.getId())
                      .identifier(simpleRecord.getIdentifier())
                      .verbatim(simpleRecord.getVerbatim())
                      .basic(MAPPER.writeValueAsString(br.orElse(null)))
                      .taxon(MAPPER.writeValueAsString(tr.orElse(null)))
                      .location(MAPPER.writeValueAsString(lr.orElse(null)))
                      .grscicoll(MAPPER.writeValueAsString(gr.orElse(null)))
                      .temporal(MAPPER.writeValueAsString(ter.orElse(null)))
                      .build();
                },
            Encoders.bean(OccurrenceSimple.class));

    interpreted.write().mode("overwrite").parquet(outputPath + "/simple-occurrence");

    // re-read
    Dataset<OccurrenceSimple> simpleRecordsReloaded =
        spark
            .read()
            .parquet(outputPath + "/simple-occurrence")
            .as(Encoders.bean(OccurrenceSimple.class));

    toJson(simpleRecordsReloaded, metadata).write().mode("overwrite").json(outputPath + "/json");

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

  public static Dataset<OccurrenceJsonRecord> toJson(
      Dataset<OccurrenceSimple> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<OccurrenceSimple, OccurrenceJsonRecord>)
            record -> {
              OccurrenceJsonConverter c =
                  OccurrenceJsonConverter.builder()
                      .metadata(metadataRecord)
                      .verbatim(
                          MAPPER.readValue((String) record.getVerbatim(), ExtendedRecord.class))
                      .basic(MAPPER.readValue((String) record.getBasic(), BasicRecord.class))
                      .location(
                          MAPPER.readValue((String) record.getLocation(), LocationRecord.class))
                      .temporal(
                          MAPPER.readValue((String) record.getTemporal(), TemporalRecord.class))
                      .multiTaxon(
                          MAPPER.readValue((String) record.getTaxon(), MultiTaxonRecord.class))
                      .grscicoll(
                          MAPPER.readValue((String) record.getGrscicoll(), GrscicollRecord.class))
                      .identifier(
                          MAPPER.readValue((String) record.getIdentifier(), IdentifierRecord.class))
                      .clustering(
                          ClusteringRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .multimedia(
                          MultimediaRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }

  public static Dataset<OccurrenceHdfsRecord> toHdfs(
      Dataset<OccurrenceSimple> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<OccurrenceSimple, OccurrenceHdfsRecord>)
            record -> {
              OccurrenceHdfsRecordConverter c =
                  OccurrenceHdfsRecordConverter.builder()
                      .metadataRecord(metadataRecord)
                      .extendedRecord(
                          MAPPER.readValue((String) record.getVerbatim(), ExtendedRecord.class))
                      .basicRecord(MAPPER.readValue((String) record.getBasic(), BasicRecord.class))
                      .locationRecord(
                          MAPPER.readValue((String) record.getLocation(), LocationRecord.class))
                      .temporalRecord(
                          MAPPER.readValue((String) record.getTemporal(), TemporalRecord.class))
                      .multiTaxonRecord(
                          MAPPER.readValue((String) record.getTaxon(), MultiTaxonRecord.class))
                      .grscicollRecord(
                          MAPPER.readValue((String) record.getGrscicoll(), GrscicollRecord.class))
                      .identifierRecord(
                          MAPPER.readValue((String) record.getIdentifier(), IdentifierRecord.class))
                      .clusteringRecord(
                          ClusteringRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .multimediaRecord(
                          MultimediaRecord.newBuilder()
                              .setId(record.getId())
                              .build()) // placeholder
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
