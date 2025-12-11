package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getFileSystem;
import static org.gbif.pipelines.interpretation.spark.SparkUtil.getSparkSession;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class CalculateDerivedMetadata {

  public static void main(String[] args) throws Exception {

    PipelinesConfig config =
        loadConfig(
            "/Users/djtfmartin/dev/my-forks/pipelines/gbif/ingestion/ingest-gbif-spark/pipelines.yaml");
    String datasetId = "ecebee66-f913-4105-acb6-738430d0edc9";
    int attempt = 1;

    String outputPath = String.format("%s/%s/%d", config.getOutputPath(), datasetId, attempt);

    SparkSession spark =
        getSparkSession("local[*]", "My app", config, Interpretation::configSparkSession);
    FileSystem fileSystem = getFileSystem(spark, config);

    runCalculateDerivedMetadata(spark, outputPath);

    fileSystem.close();
    spark.stop();
    spark.close();
    System.exit(0);
  }

  private static void runCalculateDerivedMetadata(SparkSession spark, String outputPath) {

    // loads events

    //        private java.lang.String wktConvexHull;
    //        private org.gbif.pipelines.io.avro.json.EventDate temporalCoverage;
    //        private java.util.List<org.gbif.pipelines.io.avro.json.Classification>
    // taxonomicCoverage;

  }
}
