package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class Fragmenter {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--config",
        description = "Path to YAML configuration file",
        required = false)
    private String config = "/tmp/pipelines-spark.yaml";

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

  public static void main(String[] argsv) throws IOException {

    Fragmenter.Args args = new Fragmenter.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true); // FIXME to ease airflow/registry integration
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.config);

    /* ############ standard init block ########## */
    // spark
    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(args.appName);
    if (args.master != null) {
      sparkBuilder = sparkBuilder.master(args.master);
      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");
    }
    configSparkSession(sparkBuilder, config);
    SparkSession spark = sparkBuilder.getOrCreate();

    FileSystem fileSystem;
    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    if (config.getHdfsSiteConfig() != null && config.getCoreSiteConfig() != null) {
      hadoopConf.addResource(new Path(config.getHdfsSiteConfig()));
      hadoopConf.addResource(new Path(config.getCoreSiteConfig()));
      fileSystem = FileSystem.get(hadoopConf);
    } else {
      log.warn("Using local filesystem - this is suitable for local development only");
      fileSystem = FileSystem.getLocal(hadoopConf);
    }
    /* ############ standard init block - end ########## */

    runFragmenter(
        spark, fileSystem, config, args.datasetId, args.attempt, args.appName, args.master);

    spark.stop();
    spark.close();
    fileSystem.close();
  }

  public static void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    // nothing
  }

  public static void runFragmenter(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      Integer attempt,
      String appName,
      String master) {
    log.info("Not implemented yet !");
  }
}
