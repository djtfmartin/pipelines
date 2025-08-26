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

import static org.gbif.pipelines.interpretation.spark.GrscicollInterpretation.grscicollTransform;
import static org.gbif.pipelines.interpretation.spark.HdfsView.transformToHdfsView;
import static org.gbif.pipelines.interpretation.spark.JsonView.transformToJsonView;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;
import static org.gbif.pipelines.interpretation.spark.TaxonomyInterpretation.taxonomyTransform;
import static org.gbif.pipelines.interpretation.spark.TemporalInterpretation.temporalTransform;

import java.io.IOException;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.ConfigFactory;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.BasicTransform;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;

@Slf4j
public class Interpretation implements Serializable {

  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.err.println(
          "Usage: java -jar ingest-gbif-spark-<version>.jar <config.yaml> <datasetID>");
      System.exit(1);
    }

    HdfsConfigs hdfsConfigs = HdfsConfigs.create(null, null);
    PipelinesConfig config =
              ConfigFactory.getInstance(hdfsConfigs, args[0], PipelinesConfig.class)
                      .get();

      String datasetID = args[1];
      String attempt = args[2];
      String inputPath = config.getInputPath() + "/" + datasetID + "/" + attempt;
      String outputPath = config.getOutputPath() + "/" + datasetID + "/" + attempt;

    //    SparkSession.Builder sb = SparkSession.builder();

    //    if (config.getSparkRemote() != null) {
    //      sb.remote(config.getSparkRemote());
    //    }

    //    SparkSession spark = sb.getOrCreate();

    SparkSession spark =
        SparkSession.builder()
            .appName("Run local spark")
            .master("local[*]") // Use local mode with all cores
            .getOrCreate();

    //    if (config.getJarPath() != null) {
    //      spark.addArtifact(config.getJarPath());
    //    }

    // Read the verbatim input
    log.info("Loading extended");
    Dataset<ExtendedRecord> records =
        spark.read().format("avro")
                .load(inputPath).as(Encoders.bean(ExtendedRecord.class));

      log.info("Loading identifiers");
      Dataset<IdentifierRecord> identifiers =
              spark.read().parquet(outputPath + "/identifiers").as(Encoders.bean(IdentifierRecord.class));


    // load the metadata from the registry and ES content indexes
      log.info("Loading metadata");
    MetadataServiceClient metadataServiceClient = MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(datasetID).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(datasetID, metadata);



    // Run the interpretations
    log.info("Interpreting basic");
    Dataset<BasicRecord> basic = basicTransform(config, records);
    log.info("Interpreting location");
    Dataset<LocationRecord> location = locationTransform(config, spark, records);
    log.info("Interpreting temporal");
    Dataset<TemporalRecord> temporal = temporalTransform(records);
    log.info("Interpreting taxonomy");
    Dataset<MultiTaxonRecord> taxonomy = taxonomyTransform(config, spark, records);
    log.info("Interpreting grscicoll");
    Dataset<GrscicollRecord> grscicoll = grscicollTransform(config, spark, records, metadata);

    //    Dataset<VerbatimRecord> verbatim = verbatimTransform(records);
    //    Dataset<AudubonRecord> audubon = audubonTransform(config, spark, records);
    //    Dataset<ImageRecord> image = imageTransform(config, spark, records);
    //    Dataset<DnaDerivedDataRecord> image = dnaDerivedDataTransform(config, spark, records);
    //    Dataset<MultimediaRecord> image = multimediaTransform(config, spark, records);

    // import org.gbif.pipelines.transforms.specific.ClusteringTransform;
    // import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
    // import org.gbif.pipelines.transforms.specific.GbifIdTransform;

    // Write the intermediate output (useful for debugging)
    basic.write().mode("overwrite").parquet(outputPath+ "/basic");
    location.write().mode("overwrite").parquet(outputPath+ "/location");
    temporal.write().mode("overwrite").parquet(outputPath+ "/temporal");
    taxonomy.write().mode("overwrite").parquet(outputPath+ "/taxonomy");
    grscicoll.write().mode("overwrite").parquet(outputPath + "/grscicoll");

    // hdfs
    Dataset<OccurrenceHdfsRecord> hdfsView =
        transformToHdfsView(
            records, metadata, identifiers, basic, location, taxonomy, temporal, grscicoll);

    DataFrameWriter<OccurrenceHdfsRecord> writer = hdfsView.write().mode("overwrite");
    writer.parquet(outputPath + "/hdfsview");

//    // json  - for elastic indexing
    Dataset<OccurrenceJsonRecord> jsonView =
        transformToJsonView(
            records, metadata, identifiers, basic, location, taxonomy, temporal, grscicoll);

    DataFrameWriter<OccurrenceJsonRecord> jsonWriter = jsonView.write().mode("overwrite");
    jsonWriter.parquet(outputPath + "/json");
//
    log.info("Interpretation finished");
    spark.close();
    System.exit(0);
  }

  private static Dataset<BasicRecord> basicTransform(
      PipelinesConfig config, Dataset<ExtendedRecord> source) {
    return source.map(
        (MapFunction<ExtendedRecord, BasicRecord>)
            er ->
                BasicTransform.builder()
                    .useDynamicPropertiesInterpretation(true)
                    .vocabularyApiUrl(config.getVocabularyService().getWsUrl())
                    .build()
                    .convert(er)
                    .get(),
        Encoders.bean(BasicRecord.class));
  }
}
