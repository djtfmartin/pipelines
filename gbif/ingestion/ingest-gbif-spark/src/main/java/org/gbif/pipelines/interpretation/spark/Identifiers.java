package org.gbif.pipelines.interpretation.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.ConfigFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

import java.io.Serializable;

public class Identifiers implements Serializable {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println(
                    "Usage: java -jar ingest-gbif-spark-<version>.jar <config.yaml> <datasetID> <attempt>");
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

        SparkSession spark =
                SparkSession.builder()
                        .appName("Run local spark")
                        .master("local[*]") // Use local mode with all cores
                        .getOrCreate();

        // Read the verbatim input
        Dataset<ExtendedRecord> records =
                spark.read().format("avro").load(inputPath)
                        .as(Encoders.bean(ExtendedRecord.class));

        // run the identifier transform
        Dataset<IdentifierRecord> identifiers = identifierTransform(records);

        // Write the identifiers to parquet
        identifiers.write().mode("overwrite").parquet(
                outputPath + "/identifiers");
    }

    private static Dataset<IdentifierRecord> identifierTransform(Dataset<ExtendedRecord> records) {

        // dummy implementation of identifier transform
        return records.map((MapFunction<ExtendedRecord, IdentifierRecord>)
                        er -> IdentifierRecord.newBuilder()
                                .setId(er.getId())
                                .setInternalId(er.getId()) //FIXME
                                .build(),
                Encoders.bean(IdentifierRecord.class));
    }
}
