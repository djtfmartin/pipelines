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
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.interpretation.transform.*;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Slf4j
public class PersistIdentifiers implements Serializable {

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
    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(args.appName);
    if (args.master != null && !args.master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(args.master);
    }
    SparkSession spark = sparkBuilder.getOrCreate();

    // Load validated identifiers
    processIdentifiers(spark, config, outputPath, datasetId);

    spark.close();
    System.exit(0);
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
}
