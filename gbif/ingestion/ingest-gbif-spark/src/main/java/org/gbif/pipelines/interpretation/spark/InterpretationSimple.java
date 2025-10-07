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
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.OBJECT_MAPPER;
import static org.gbif.pipelines.interpretation.spark.LocationInterpretation.locationTransform;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.interpretation.transform.LocationTransform;
import org.gbif.pipelines.interpretation.transform.MultiTaxonomyTransform;
import org.gbif.pipelines.io.avro.*;
import scala.Tuple3;

@Slf4j
public class InterpretationSimple implements Serializable {

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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      System.out.println("Starting a new loop: " + i);
      main2(argsv, i);
      Thread.sleep(60000);
    }
    System.exit(0);
  }

  public static void main2(String[] argsv, int loop) {
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

    // A single call to the registry to get th dataset metadata
    MetadataServiceClient metadataServiceClient =
        MetadataServiceClient.create(config.getGbifApi(), config.getContent());
    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetKey(args.datasetId).build();
    MetadataInterpreter.interpret(metadataServiceClient).accept(args.datasetId, metadata);

    // Set up our transforms
    MultiTaxonomyTransform taxonomyTransform =
        MultiTaxonomyTransform.builder().config(config).build();
    LocationTransform locationTransform = LocationTransform.builder().config(config).build();

    // Loop over all records and interpret them
    Dataset<Tuple3<String, String, String>> interpreted =
        extendedRecords.map(
            (MapFunction<ExtendedRecord, Tuple3<String, String, String>>)
                er -> {
                  Optional<MultiTaxonRecord> tr = taxonomyTransform.convert(er);
                  Optional<LocationRecord> lr = locationTransform.convert(er, metadata);
                  return Tuple3.apply(
                      OBJECT_MAPPER.writeValueAsString(er),
                      OBJECT_MAPPER.writeValueAsString(tr.orElse(null)),
                      OBJECT_MAPPER.writeValueAsString(lr.orElse(null)));
                },
            Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

    interpreted
        .write()
        .mode("overwrite")
        .parquet(outputPath + "/simple-interp-" + loop + ".parquet");

    // Convert the interpreted and write the JSON file

    // Convert the interpreted and write the Parquet file for HDFS view

    spark.close();
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
}
