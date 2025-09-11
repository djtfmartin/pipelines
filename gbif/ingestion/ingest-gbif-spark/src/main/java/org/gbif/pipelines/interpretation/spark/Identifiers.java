package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.interpretation.transform.GbifIdTransform;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Slf4j
public class Identifiers implements Serializable {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--tripletValid",
        description = "Is triplet valid",
        required = false,
        arity = 1)
    private boolean tripletValid = false;

    @Parameter(
        names = "--occurrenceIdValid",
        description = "Is occurrence ID valid",
        required = false,
        arity = 1)
    private boolean occurrenceIdValid = true;

    @Parameter(names = "--coreSiteConfig", description = "Path to core-site.xml", required = false)
    private String coreSiteConfig;

    @Parameter(names = "--hdfsSiteConfig", description = "Path to hdfs-site.xml", required = false)
    private String hdfsSiteConfig;

    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    private int numberOfShards;

    @Parameter(names = "--properties", description = "Path to properties file", required = true)
    private String properties;

    @Parameter(
        names = "--master",
        description = "Spark master - there for local dev only",
        required = false)
    private String master;

    @Parameter(
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    boolean help;
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

    // Read the verbatim input
    Dataset<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(inputPath + "/verbatim.avro")
            .repartition(args.numberOfShards)
            .as(Encoders.bean(ExtendedRecord.class));

    // run the identifier transform
    Dataset<IdentifierRecord> identifiers = identifierTransform(spark, config, datasetID, records);

    // Write the identifiers to parquet
    identifiers.write().mode("overwrite").parquet(outputPath + "/identifiers");

    log.info("Identifiers finished");
    spark.close();
    System.exit(0);
  }

  private static Dataset<IdentifierRecord> identifierTransform(
      final SparkSession spark,
      final PipelinesConfig config,
      final String datasetId,
      Dataset<ExtendedRecord> records) {

    LongAccumulator processedRecord = spark.sparkContext().longAccumulator("Processed-records");

    GbifIdTransform transform =
        GbifIdTransform.builder()
            .keygenServiceSupplier(
                (SerializableSupplier<HBaseLockingKey>)
                    () -> KeygenServiceFactory.create(config, datasetId))
            .build();

    return records.map(
        (MapFunction<ExtendedRecord, IdentifierRecord>)
            er -> {
              processedRecord.add(1L);
              return transform.convert(er).get();
            },
        Encoders.bean(IdentifierRecord.class));
  }
}
