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
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformToHdfsView;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.Serializable;
import java.util.function.BiConsumer;
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

    String datasetID = args.datasetId;
    int attempt = args.attempt;
    String inputPath = config.getInputPath() + "/" + datasetID + "/" + attempt;
    String outputPath = config.getOutputPath() + "/" + datasetID + "/" + attempt;

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(args.appName);
    if (args.master != null && !args.master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(args.master);
    }
    SparkSession spark = sparkBuilder.getOrCreate();

    log.info("=== Step 1: Load extended records from {}", inputPath);
    Dataset<ExtendedRecord> extendedRecords = loadExtendedRecords(spark, inputPath);
    Dataset<OccurrenceRecord> occurrenceRecords =
        extendedRecords.map(
            new ExtendedToOccurrenceMapper(), Encoders.bean(OccurrenceRecord.class));

    log.info("=== Step 2: Load metadata from registry and ES");
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(datasetID).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(datasetID, metadata);

    log.info("=== Step 3: Load identifiers from {}", outputPath);
    Dataset<IdentifierRecord> identifiers = loadIdentifiers(spark, outputPath);

    log.info("=== Step 4: Interpret basic terms");
    Dataset<BasicRecord> basic = basicTransform(config, extendedRecords);
    writeDebug(basic, outputPath, "basic", args.debug);

    log.info("=== Step 5: Interpret location");
    Dataset<LocationRecord> location = locationTransform(config, spark, extendedRecords, metadata);
    writeDebug(location, outputPath, "location", args.debug);

    log.info("=== Step 6: Interpret temporal");
    Dataset<TemporalRecord> temporal = temporalTransform(extendedRecords);
    writeDebug(temporal, outputPath, "temporal", args.debug);

    log.info("=== Step 7: Interpret taxonomy");
    Dataset<MultiTaxonRecord> multiTaxon = taxonomyTransform(config, spark, extendedRecords);
    writeDebug(multiTaxon, outputPath, "taxonomy", args.debug);

    log.info("=== Step 8: Interpret GrSciColl");
    Dataset<GrscicollRecord> grscicoll =
        grscicollTransform(config, spark, extendedRecords, metadata);
    writeDebug(grscicoll, outputPath, "grscicoll", args.debug);

    // Join all interpreted datasets into occurrence
    occurrenceRecords = joinTo(occurrenceRecords, identifiers, OccurrenceRecord::setIdentifier);
    occurrenceRecords = joinTo(occurrenceRecords, basic, OccurrenceRecord::setBasic);
    occurrenceRecords = joinTo(occurrenceRecords, location, OccurrenceRecord::setLocation);
    occurrenceRecords = joinTo(occurrenceRecords, temporal, OccurrenceRecord::setTemporal);
    occurrenceRecords = joinTo(occurrenceRecords, multiTaxon, OccurrenceRecord::setMultiTaxon);
    occurrenceRecords = joinTo(occurrenceRecords, grscicoll, OccurrenceRecord::setGrscicoll);

    if (args.hdfsView) {
      log.info("=== Step 9: Generate HDFS view");
      Dataset<OccurrenceHdfsRecord> hdfsView = transformToHdfsView(occurrenceRecords, metadata);
      hdfsView.write().mode("overwrite").parquet(outputPath + "/hdfsview");
    }
    //
    //    if (args.jsonView) {
    //      log.info("=== Step 10: Generate JSON view");
    //      Dataset<OccurrenceJsonRecord> jsonView = transformToJsonView(occurrenceRecords,
    // metadata);
    //      jsonView.write().mode("overwrite").parquet(outputPath + "/json");
    //    }

    log.info("=== Interpretation pipeline finished successfully ===");
    spark.close();
    System.exit(0);
  }

  // ----------------- Helper Methods -----------------

  private static Dataset<ExtendedRecord> loadExtendedRecords(SparkSession spark, String inputPath) {
    return spark
        .read()
        .format("avro")
        .load(inputPath + "/verbatim.avro")
        .as(Encoders.bean(ExtendedRecord.class))
        .repartition(10);
  }

  private static Dataset<IdentifierRecord> loadIdentifiers(SparkSession spark, String outputPath) {
    return spark
        .read()
        .parquet(outputPath + "/identifiers")
        .as(Encoders.bean(IdentifierRecord.class));
  }

  private static <T> void writeDebug(
      Dataset<? extends Record> records, String outputPath, String name, boolean debug) {

    if (debug) {
      log.info("Writing debug {}", name);
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

  private static Dataset<BasicRecord> basicTransform(
      PipelinesConfig config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, BasicRecord>)
            er -> {
              return BasicTransform.builder()
                  .useDynamicPropertiesInterpretation(true)
                  .vocabularyApiUrl(config.getVocabularyService().getWsUrl())
                  .build()
                  .convert(er)
                  .get();
            },
        Encoders.bean(BasicRecord.class));
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
}
