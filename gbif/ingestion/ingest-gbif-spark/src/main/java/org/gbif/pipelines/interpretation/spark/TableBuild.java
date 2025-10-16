package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;

public class TableBuild {

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
        names = {"--help", "-h"},
        help = true,
        description = "Show usage")
    private boolean help;
  }

  public static void main(String[] argsv) {
    TableBuild.Args args = new TableBuild.Args();
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

    sparkBuilder
        .enableHiveSupport()
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    SparkSession spark = sparkBuilder.getOrCreate();

    // load hdfs view
    Dataset<OccurrenceHdfsRecord> hdfs =
        spark.read().parquet(outputPath + "/hdfs").as(Encoders.bean(OccurrenceHdfsRecord.class));

    String table = "occurrence_" + datasetId.replace("-", "_") + "_" + attempt;

    spark.sql("use dave");
    hdfs.writeTo(table).create();

    System.out.println("Created Iceberg table:");
    spark.sql("DESCRIBE TABLE " + table).show(false);

    //    spark.sql(
    //        "CREATE EXTERNAL TABLE naturalis "
    //            + "USING iceberg  "
    //            + "LOCATION '/data/ingest_spark/15f819bd-6612-4447-854b-14d12ee1022d/158/iceberg'
    // "
    //            + "AS SELECT * FROM
    // parquet.`/data/ingest_spark/15f819bd-6612-4447-854b-14d12ee1022d/158/hdfs`");

    spark.close();
  }
}
