package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
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
    runFragmenter(config, args.datasetId, args.attempt, args.appName, args.master);
  }

  public static void runFragmenter(
      PipelinesConfig config, String datasetId, Integer attempt, String appName, String master) {

    String inputPath =
        String.format("%s/%s/%d/%s", config.getOutputPath(), datasetId, attempt, "json");

    // Create Spark session
    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(appName);

    if (master != null && !master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(master);
      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    if (master != null) {
      Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
      hadoopConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
      hadoopConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    }

    log.info("Not implemented yet !");
  }
}
