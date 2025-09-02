package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import java.io.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.interpretation.transform.GbifIdTransform;
import org.gbif.pipelines.interpretation.transform.utils.KeygenServiceFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;

@Slf4j
public class Identifiers implements Serializable {

  public static void main(String[] args) {

    if (args.length < 3) {
      System.err.println(
          "Usage: java -jar ingest-gbif-spark-<version>.jar <config.yaml> <datasetID> <attempt>");
      System.exit(1);
    }

    PipelinesConfig config = loadConfig(args[0]);

    String datasetID = args[1];
    String attempt = args[2];
    String inputPath = "file://" + config.getInputPath() + "/" + datasetID + "/" + attempt;
    String outputPath = config.getOutputPath() + "/" + datasetID + "/" + attempt;

    SparkSession spark =
        SparkSession.builder()
            .appName("Run local spark")
            .master("local[*]") // Use local mode with all cores
            .getOrCreate();

    // Read the verbatim input
    Dataset<ExtendedRecord> records =
        spark.read().format("avro").load(inputPath).as(Encoders.bean(ExtendedRecord.class));

    // run the identifier transform
    Dataset<IdentifierRecord> identifiers = identifierTransform(config, datasetID, records);

    // Write the identifiers to parquet
    identifiers.write().mode("overwrite").parquet("file://" + outputPath + "/identifiers");

    log.info("Identifers finished");
    spark.close();
    System.exit(0);
  }

  private static Dataset<IdentifierRecord> identifierTransform(
      final PipelinesConfig config, final String datasetId, Dataset<ExtendedRecord> records) {

    GbifIdTransform transform =
        GbifIdTransform.builder()
            .keygenServiceSupplier(
                new SerializableSupplier<HBaseLockingKey>() {
                  @Override
                  public HBaseLockingKey get() {
                    return KeygenServiceFactory.create(config, datasetId);
                  }
                })
            .build();

    return records
        .repartition(config.getKeygen().getParallelism())
        .map(
            (MapFunction<ExtendedRecord, IdentifierRecord>) er -> transform.convert(er).get(),
            Encoders.bean(IdentifierRecord.class));
  }
}
