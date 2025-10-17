package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

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
    SparkConf sparkConf =
        new SparkConf().set("hive.metastore.warehouse.dir", "hdfs://gbif-hdfs/stackable/warehouse");
    sparkBuilder
        .enableHiveSupport()
        .config(sparkConf)
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");

    SparkSession spark = sparkBuilder.getOrCreate();

    // load hdfs view
    Dataset<Row> hdfs = spark.read().parquet(outputPath + "/hdfs");
    String[] columns = hdfs.columns();

    StringBuilder selectBuffer = new StringBuilder();

    for (int i = 0; i < columns.length; i++) {
      if (columns[i].matches("^v[A-Z].*") || columns[i].matches("^V[A-Z].*")) {
        String icebergCol = "v_" + columns[i].substring(1).toLowerCase().replaceAll("\\$", "");
        selectBuffer.append("`" + columns[i] + "` AS " + icebergCol);
      } else {
        selectBuffer.append(
            "`" + columns[i] + "` AS " + columns[i].toLowerCase().replaceAll("\\$", ""));
      }
      if (i < columns.length - 1) {
        selectBuffer.append(", ");
      }
    }

    String table = "occurrence_" + datasetId.replace("-", "_") + "_" + attempt;

    spark.sql("use dave");

    spark.sql("DROP TABLE IF EXISTS " + table);

    hdfs.writeTo(table).create();

    System.out.println("Created Iceberg table:");
    spark.sql("DESCRIBE TABLE " + table).show(false);

    spark.sql("select count(*) from " + table).show(false);

    String insertQuery =
        "INSERT OVERWRITE TABLE dave.occurrence PARTITION (dataset_key = '"
            + datasetId
            + "') "
            + "SELECT "
            + selectBuffer.toString()
            + " FROM dave."
            + table;

    System.out.println("Inserting data into occurrence table: " + insertQuery);

    spark.sql(insertQuery);

    spark.sql("DROP TABLE " + table);

    spark.close();
  }
}
