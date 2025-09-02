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
import static org.gbif.pipelines.interpretation.spark.GrscicollInterpretation.grscicollTransform;
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformToHdfsView;
import static org.gbif.pipelines.interpretation.spark.JsonView.transformToJsonView;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
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

    //    SparkSession.Builder sb = SparkSession.builder();

    //    if (config.getSparkRemote() != null) {
    //      sb.remote(config.getSparkRemote());
    //    }

    //    SparkSession spark = sb.getOrCreate();

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(args.appName);

    if (args.master != null && !args.master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(args.master);
    }

    SparkSession spark = sparkBuilder.getOrCreate();

    //    if (config.getJarPath() != null) {
    //      spark.addArtifact(config.getJarPath());
    //    }

    // Read the verbatim input
    log.info("Loading extended");
    Dataset<OccurrenceRecord> records =
        spark
            .read()
            .format("avro")
            .load(inputPath + "/verbatim.avro")
            .as(Encoders.bean(ExtendedRecord.class))
            .repartition(10)
            .map(
                (MapFunction<ExtendedRecord, OccurrenceRecord>)
                    extendedRecord -> OccurrenceRecord.builder().verbatim(extendedRecord).build(),
                Encoders.bean(OccurrenceRecord.class));

    log.info("Loading identifiers");
    Dataset<IdentifierRecord> identifiers =
        spark.read().parquet(outputPath + "/identifiers").as(Encoders.bean(IdentifierRecord.class));

    records = joinIdentifiers(records, identifiers);

    // load the metadata from the registry and ES content indexes
    log.info("Loading metadata");

    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(datasetID).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(datasetID, metadata);

    // Run the interpretations
    log.info("Interpreting basic");
    records = basicTransform(config, records);

    if (args.debug) {
      log.info("Writing debug basic");
      records
          .map(
              (MapFunction<OccurrenceRecord, BasicRecord>) OccurrenceRecord::getBasic,
              Encoders.bean(BasicRecord.class))
          .write()
          .mode("overwrite")
          .parquet(outputPath + "/basic");
    }

    log.info("Interpreting location");
    records = locationTransform(config, spark, records);

    if (args.debug) {
      log.info("Writing debug location");
      records
          .map(
              (MapFunction<OccurrenceRecord, LocationRecord>) OccurrenceRecord::getLocation,
              Encoders.bean(LocationRecord.class))
          .write()
          .mode("overwrite")
          .parquet(outputPath + "/location");
    }

    log.info("Interpreting temporal");
    records = temporalTransform(records);
    if (args.debug) {
      log.info("Writing debug temporal");
      records
          .map(
              (MapFunction<OccurrenceRecord, TemporalRecord>) OccurrenceRecord::getTemporal,
              Encoders.bean(TemporalRecord.class))
          .write()
          .mode("overwrite")
          .parquet(outputPath + "/temporal");
    }

    log.info("Interpreting taxonomy");
    records = taxonomyTransform(config, spark, records);

    if (args.debug) {
      log.info("Writing debug taxonomy");
      records
          .map(
              (MapFunction<OccurrenceRecord, MultiTaxonRecord>) OccurrenceRecord::getMultiTaxon,
              Encoders.bean(MultiTaxonRecord.class))
          .write()
          .mode("overwrite")
          .parquet(outputPath + "/taxonomy");
    }

    log.info("Interpreting grscicoll");
    records = grscicollTransform(config, spark, records, metadata);
    if (args.debug) {
      log.info("Writing debug grscicoll");
      records
          .map(
              (MapFunction<OccurrenceRecord, GrscicollRecord>) OccurrenceRecord::getGrscicoll,
              Encoders.bean(GrscicollRecord.class))
          .write()
          .mode("overwrite")
          .parquet(outputPath + "/grscicoll");
    }

    //    //    Dataset<VerbatimRecord> verbatim = verbatimTransform(records);
    //    //    Dataset<AudubonRecord> audubon = audubonTransform(config, spark, records);
    //    //    Dataset<ImageRecord> image = imageTransform(config, spark, records);
    //    //    Dataset<DnaDerivedDataRecord> image = dnaDerivedDataTransform(config, spark,
    // records);
    //    //    Dataset<MultimediaRecord> image = multimediaTransform(config, spark, records);
    //

    //  hdfs
    if (args.hdfsView) {
      log.info("Generating hdfs view");
      Dataset<OccurrenceHdfsRecord> hdfsView = transformToHdfsView(records, metadata);
      DataFrameWriter<OccurrenceHdfsRecord> writer = hdfsView.write().mode("overwrite");
      writer.parquet(outputPath + "/hdfsview");
    }

    // json
    log.info("Generating json view");
    if (args.jsonView) {
      Dataset<OccurrenceJsonRecord> jsonView = transformToJsonView(records, metadata);
      DataFrameWriter<OccurrenceJsonRecord> jsonWriter = jsonView.write().mode("overwrite");
      jsonWriter.parquet(outputPath + "/json");
    }

    log.info("Interpretation finished");
    spark.close();
    System.exit(0);
  }

  private static Dataset<OccurrenceRecord> joinIdentifiers(
      Dataset<OccurrenceRecord> records, Dataset<IdentifierRecord> identifiers) {
    return records
        .joinWith(identifiers, records.col("verbatim.id").equalTo(identifiers.col("id")))
        .map(
            (MapFunction<Tuple2<OccurrenceRecord, IdentifierRecord>, OccurrenceRecord>)
                row -> {
                  OccurrenceRecord r = row._1;
                  r.setIdentifier(row._2);
                  return r;
                },
            Encoders.bean(OccurrenceRecord.class));
  }

  private static Dataset<OccurrenceRecord> basicTransform(
      PipelinesConfig config, Dataset<OccurrenceRecord> source) {
    return source.map(
        (MapFunction<OccurrenceRecord, OccurrenceRecord>)
            er -> {
              er.setBasic(
                  BasicTransform.builder()
                      .useDynamicPropertiesInterpretation(true)
                      .vocabularyApiUrl(config.getVocabularyService().getWsUrl())
                      .build()
                      .convert(er.getVerbatim())
                      .get());
              return er;
            },
        Encoders.bean(OccurrenceRecord.class));
  }
}
