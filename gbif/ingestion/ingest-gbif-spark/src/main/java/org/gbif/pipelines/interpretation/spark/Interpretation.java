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
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.*;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
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

    // Load the extended records
    Dataset<ExtendedRecord> extendedRecords =
        loadExtendedRecords(spark, config, inputPath, outputPath, args.numberOfShards);

    // Process and then reload identifiers
    processIdentifiers(spark, config, outputPath, datasetId);
    Dataset<IdentifierRecord> identifiers = loadIdentifiers(spark, outputPath);

    // join and write to disk
    extendedRecords
        .as("extendedRecord")
        .joinWith(identifiers, extendedRecords.col("id").equalTo(identifiers.col("id")))
        .map(
            (MapFunction<Tuple2<ExtendedRecord, IdentifierRecord>, Occurrence>)
                row -> {
                  ExtendedRecord er = row._1;
                  IdentifierRecord ir = row._2;
                  return Occurrence.builder()
                      .id(er.getId())
                      .verbatim(MAPPER.writeValueAsString(er))
                      .identifier(MAPPER.writeValueAsString(ir))
                      .build();
                },
            Encoders.bean(Occurrence.class))
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/extended-identifiers");

    // A single call to the registry to get the dataset metadata
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    final MetadataRecord metadata =
        MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    Dataset<Occurrence> simpleRecords =
        spark
            .read()
            .parquet(outputPath + "/extended-identifiers")
            .as(Encoders.bean(Occurrence.class));

    Dataset<Occurrence> interpreted = runTransforms(config, simpleRecords, metadata);

    interpreted.write().mode("overwrite").parquet(outputPath + "/simple-occurrence");

    // re-load
    Dataset<Occurrence> simpleRecordsReloaded =
        spark.read().parquet(outputPath + "/simple-occurrence").as(Encoders.bean(Occurrence.class));

    // write parquet for elastic
    toJson(simpleRecordsReloaded, metadata).write().mode("overwrite").parquet(outputPath + "/json");

    // write parquet for hdfs view
    toHdfs(simpleRecordsReloaded, metadata).write().mode("overwrite").parquet(outputPath + "/hdfs");

    // cleanup
    HdfsConfigs hdfsConfigs = HdfsConfigs.create(args.hdfsSiteConfig, args.coreSiteConfig);
    FsUtils.deleteIfExist(hdfsConfigs, outputPath + "/extended-identifiers");
    FsUtils.deleteIfExist(hdfsConfigs, outputPath + "/simple-occurrence");

    spark.close();
    System.exit(0);
  }

  private static Dataset<Occurrence> runTransforms(
      PipelinesConfig config, Dataset<Occurrence> simpleRecords, MetadataRecord metadata) {

    // Set up our transforms
    DefaultValuesTransform defaultValuesTransform = DefaultValuesTransform.create(config, metadata);
    MultiTaxonomyTransform taxonomyTransform = MultiTaxonomyTransform.create(config);
    LocationTransform locationTransform = LocationTransform.create(config);
    GrscicollTransform grscicollTransform = GrscicollTransform.create(config);
    TemporalTransform temporalTransform = TemporalTransform.create(config);
    BasicTransform basicTransform = BasicTransform.create(config);
    DnDerivedDataTransform dnDerivedDataTransform = DnDerivedDataTransform.create();
    MultimediaTransform multimediaTransform = MultimediaTransform.create(config);
    ImageTransform imageTransform = ImageTransform.create(config);
    AudubonTransform audubonTransform = AudubonTransform.create(config);
    ClusteringTransform clusteringTransform = ClusteringTransform.create(config);

    // Loop over all records and interpret them
    return simpleRecords.map(
        (MapFunction<Occurrence, Occurrence>)
            simpleRecord -> {
              ExtendedRecord er =
                  MAPPER.readValue(simpleRecord.getVerbatim(), ExtendedRecord.class);
              IdentifierRecord idr =
                  MAPPER.readValue(simpleRecord.getIdentifier(), IdentifierRecord.class);

              // Apply all transforms
              er = defaultValuesTransform.convert(er);
              MultiTaxonRecord tr = taxonomyTransform.convert(er);
              LocationRecord lr = locationTransform.convert(er, metadata);
              GrscicollRecord gr = grscicollTransform.convert(er, metadata);
              TemporalRecord ter = temporalTransform.convert(er);
              BasicRecord br = basicTransform.convert(er);
              DnaDerivedDataRecord dr = dnDerivedDataTransform.convert(er);
              Optional<MultimediaRecord> mr = multimediaTransform.convert(er);
              Optional<ImageRecord> ir = imageTransform.convert(er);
              Optional<AudubonRecord> ar = audubonTransform.convert(er);
              ClusteringRecord cr = clusteringTransform.convert(idr);

              // merge the multimedia records
              MultimediaRecord mmr =
                  MultimediaConverter.merge(
                      mr.orElse(MultimediaRecord.newBuilder().setId(er.getId()).build()),
                      ir.orElse(ImageRecord.newBuilder().setId(er.getId()).build()),
                      ar.orElse(AudubonRecord.newBuilder().setId(er.getId()).build()));

              return Occurrence.builder()
                  .id(er.getId())
                  .identifier(simpleRecord.getIdentifier())
                  .verbatim(simpleRecord.getVerbatim())
                  .basic(MAPPER.writeValueAsString(br))
                  .taxon(MAPPER.writeValueAsString(tr))
                  .location(MAPPER.writeValueAsString(lr))
                  .grscicoll(MAPPER.writeValueAsString(gr))
                  .temporal(MAPPER.writeValueAsString(ter))
                  .dnaDerivedData(MAPPER.writeValueAsString(dr))
                  .multimedia(MAPPER.writeValueAsString(mmr))
                  .clustering(MAPPER.writeValueAsString(cr))
                  .build();
            },
        Encoders.bean(Occurrence.class));
  }

  // ----------------- Helper Methods -----------------

  private static Dataset<ExtendedRecord> loadExtendedRecords(
      SparkSession spark,
      PipelinesConfig config,
      String inputPath,
      String outputPath,
      int numberOfShards) {

    spark
        .sparkContext()
        .setJobGroup("load-avro", String.format("Load extended records from %s", inputPath), true);

    final Set<String> allowExtensions =
        Optional.ofNullable(config.getExtensionsAllowedForVerbatimSet())
            .orElse(Collections.emptySet());

    Dataset<ExtendedRecord> extended =
        spark
            .read()
            .format("avro")
            .load(inputPath + "/verbatim.avro")
            .as(Encoders.bean(ExtendedRecord.class))
            .filter(
                (FilterFunction<ExtendedRecord>) er -> er != null && !er.getCoreTerms().isEmpty())
            .map(
                (MapFunction<ExtendedRecord, ExtendedRecord>)
                    er -> {
                      Map<String, List<Map<String, String>>> extensions = new HashMap<>();
                      er.getExtensions().entrySet().stream()
                          .filter(es -> allowExtensions.contains(es.getKey()))
                          .filter(es -> !es.getValue().isEmpty())
                          .forEach(es -> extensions.put(es.getKey(), es.getValue()));
                      return ExtendedRecord.newBuilder()
                          .setId(er.getId())
                          .setCoreTerms(er.getCoreTerms())
                          .setExtensions(extensions)
                          .build();
                    },
                Encoders.bean(ExtendedRecord.class))
            .repartition(numberOfShards);

    // write to parquet for debug purposes
    extended.write().mode("overwrite").parquet(outputPath + "/verbatim_ext_filtered");

    // reload
    return spark
        .read()
        .parquet(outputPath + "/verbatim_ext_filtered")
        .as(Encoders.bean(ExtendedRecord.class));
  }

  private static Dataset<IdentifierRecord> loadIdentifiers(SparkSession spark, String outputPath) {

    return spark
        .read()
        .parquet(outputPath + "/identifiers")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  public static void processIdentifiers(
      SparkSession spark, PipelinesConfig config, String outputPath, String datasetId) {

    // load the valid identifiers - identifiers already present in hbase
    Dataset<IdentifierRecord> identifiers = loadValidIdentifiers(spark, outputPath);

    // persist the absent identifiers - records that need to be assigned an identifier (added to
    // hbase)
    Dataset<IdentifierRecord> newlyAdded =
        persistAbsentIdentifiers(spark, outputPath, config, datasetId);

    // merge the two datasets
    Dataset<IdentifierRecord> allIdentifiers = identifiers.union(newlyAdded);

    // write out the final identifiers
    allIdentifiers.write().mode("overwrite").parquet(outputPath + "/identifiers");
  }

  private static Dataset<IdentifierRecord> persistAbsentIdentifiers(
      SparkSession spark, String outputPath, PipelinesConfig config, String datasetId) {

    Dataset<IdentifierRecord> absentIdentifiers =
        spark
            .read()
            .parquet(outputPath + "/identifiers_absent")
            .as(Encoders.bean(IdentifierRecord.class));

    GbifAbsentIdTransform absentIdTransform =
        GbifAbsentIdTransform.builder()
            .isTripletValid(true) // set according to your validation logic
            .isOccurrenceIdValid(true) // set according to your validation logic
            .useExtendedRecordId(false) // set according to your use case
            .generateIdIfAbsent(true)
            .keygenServiceSupplier(
                (SerializableSupplier<HBaseLockingKey>)
                    () ->
                        KeygenServiceFactory.create(
                            config, datasetId)) // replace with actual config and dataset ID
            .build();

    // Persist to HBase or any other storage
    return absentIdentifiers.map(
        (MapFunction<IdentifierRecord, IdentifierRecord>) absentIdTransform::persist,
        Encoders.bean(IdentifierRecord.class));
  }

  private static Dataset<IdentifierRecord> loadValidIdentifiers(
      SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/identifiers_valid")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  public static Dataset<OccurrenceJsonRecord> toJson(
      Dataset<Occurrence> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<Occurrence, OccurrenceJsonRecord>)
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
                      .multimedia(
                          MAPPER.readValue((String) record.getMultimedia(), MultimediaRecord.class))
                      .dnaDerivedData(
                          MAPPER.readValue(
                              (String) record.getDnaDerivedData(), DnaDerivedDataRecord.class))
                      .clustering(
                          MAPPER.readValue((String) record.getClustering(), ClusteringRecord.class))
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceJsonRecord.class));
  }

  public static Dataset<OccurrenceHdfsRecord> toHdfs(
      Dataset<Occurrence> records, MetadataRecord metadataRecord) {
    return records.map(
        (MapFunction<Occurrence, OccurrenceHdfsRecord>)
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
                      .multimediaRecord(
                          MAPPER.readValue((String) record.getMultimedia(), MultimediaRecord.class))
                      .dnaDerivedDataRecord(
                          MAPPER.readValue(
                              (String) record.getDnaDerivedData(), DnaDerivedDataRecord.class))
                      .clusteringRecord(
                          MAPPER.readValue((String) record.getClustering(), ClusteringRecord.class))
                      .build();

              return c.convert();
            },
        Encoders.bean(OccurrenceHdfsRecord.class));
  }
}
