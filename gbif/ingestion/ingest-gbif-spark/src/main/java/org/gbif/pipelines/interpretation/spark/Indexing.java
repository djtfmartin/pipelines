package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.EsConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.EsIndexUtils;

/**
 * Main class for indexing occurrence data to Elasticsearch. It reads Parquet files from HDFS,
 * creates an Elasticsearch index if it doesn't exist, deletes existing records for a specific
 * dataset ID, and writes new records to the index.
 */
public class Indexing {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--esIndexName",
        description = "Name of the Elasticsearch index that will be used to index the records")
    private String esIndexName;

    @Parameter(
        names = "--indexNumberShards",
        description = "Number of primary shards in the target index. Default = 3")
    private Integer indexNumberShards = 3;

    @Parameter(
        names = "--indexNumberReplicas",
        description = "Number of replica shards per primary shard in the target index. Default = 1")
    private Integer indexNumberReplicas = 1;

    @Parameter(names = "--properties", description = "Path to YAML file", required = true)
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

  public static void main(String[] argsv) throws IOException {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.properties);
    runIndexing(
        config,
        args.datasetId,
        args.attempt,
        args.appName,
        args.master,
        args.esIndexName,
        args.indexNumberShards,
        args.indexNumberReplicas);
  }

  public static void runIndexing(
      PipelinesConfig config,
      String datasetId,
      Integer attempt,
      String appName,
      String master,
      String esIndexName,
      Integer indexNumberShards,
      Integer indexNumberReplicas) {

    String inputPath =
        String.format("%s/%s/%d/%s", config.getOutputPath(), datasetId, attempt, "json");

    // Create Spark session
    SparkSession.Builder sparkBuilder =
        SparkSession.builder()
            .appName(appName)
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1")
            .config("es.nodes", String.join(",", config.getElastic().getEsHosts()));

    if (master != null && !master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(master);
    }

    SparkSession spark = sparkBuilder.getOrCreate();

    ElasticOptions options =
        ElasticOptions.fromArgsAndConfig(
            config, esIndexName, datasetId, attempt, indexNumberShards, indexNumberReplicas);

    // Create ES index and alias if not exists
    EsIndexUtils.createIndexAndAliasForDefault(options);

    // Returns indices names in case of swapping
    Set<String> indices = EsIndexUtils.deleteRecordsByDatasetId(options);

    // Read parquet files
    Dataset<Row> df = spark.read().parquet(inputPath);

    // Write to Elasticsearch
    df.write()
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", esIndexName)
        .option("es.batch.size.entries", config.getElastic().getEsMaxBatchSize())
        .option("es.batch.size.bytes", config.getElastic().getEsMaxBatchSizeBytes())
        .option("es.mapping.id", "id")
        .option("es.nodes.wan.only", "true")
        .option("es.batch.write.refresh", "false")
        .mode("append")
        .save();

    spark.stop();

    EsIndexUtils.updateAlias(options, indices, config.getIndexLock());
    EsIndexUtils.refreshIndex(options);
  }

  @Builder
  @Data
  public static class ElasticOptions {
    String esSchemaPath;
    String esIndexName;
    String[] esAlias;
    String[] esHosts;
    String datasetId;
    Integer attempt;
    @Builder.Default Integer indexNumberShards = 1;
    @Builder.Default String indexRefreshInterval = "40s";
    @Builder.Default Integer indexNumberReplicas = 1;
    @Builder.Default Integer indexMaxResultWindow = 200000;
    @Builder.Default String unassignedNodeDelay = "5m";
    @Builder.Default Boolean useSlowlog = true;
    @Builder.Default String indexSearchSlowlogThresholdQueryWarn = "20s";
    @Builder.Default String indexSearchSlowlogThresholdQueryInfo = "10s";
    @Builder.Default String indexSearchSlowlogThresholdFetchWarn = "2s";
    @Builder.Default String indexSearchSlowlogThresholdFetchInfo = "1s";
    @Builder.Default String indexSearchSlowlogLevel = "info";
    @Builder.Default Integer searchQueryTimeoutSec = 5;
    @Builder.Default Integer searchQueryAttempts = 200;

    public static ElasticOptions fromArgsAndConfig(
        PipelinesConfig config,
        String esIndexName,
        String datasetId,
        Integer attempt,
        Integer indexNumberShards,
        Integer indexNumberReplicas) {
      EsConfig esConfig = config.getElastic();
      ElasticOptionsBuilder builder =
          ElasticOptions.builder()
              .esIndexName(esIndexName)
              .indexNumberShards(indexNumberShards)
              .indexNumberReplicas(indexNumberReplicas)
              .esAlias(new String[] {esConfig.getEsAlias()})
              .datasetId(datasetId)
              .attempt(attempt)
              .esSchemaPath(esConfig.getEsSchemaPath())
              .esHosts(esConfig.getEsHosts().split(","));

      if (esConfig.getIndexRefreshInterval() != null) {
        builder.indexRefreshInterval(esConfig.getIndexRefreshInterval());
      }
      if (esConfig.getUnassignedNodeDelay() != null) {
        builder.unassignedNodeDelay(esConfig.getUnassignedNodeDelay());
      }
      if (esConfig.getIndexSearchSlowlogThresholdQueryWarn() != null) {
        builder.indexSearchSlowlogLevel(esConfig.getIndexSearchSlowlogThresholdQueryWarn());
      }
      if (esConfig.getIndexSearchSlowlogThresholdQueryInfo() != null) {
        builder.indexSearchSlowlogThresholdQueryInfo(
            esConfig.getIndexSearchSlowlogThresholdQueryInfo());
      }
      if (esConfig.getIndexSearchSlowlogThresholdFetchWarn() != null) {
        builder.indexSearchSlowlogThresholdFetchWarn(
            esConfig.getIndexSearchSlowlogThresholdFetchWarn());
      }
      if (esConfig.getIndexSearchSlowlogThresholdFetchInfo() != null) {
        builder.indexSearchSlowlogThresholdFetchInfo(
            esConfig.getIndexSearchSlowlogThresholdFetchInfo());
      }
      if (esConfig.getIndexSearchSlowlogLevel() != null) {
        builder.indexSearchSlowlogLevel(esConfig.getIndexSearchSlowlogLevel());
      }
      if (esConfig.getSearchQueryTimeoutSec() != null) {
        builder.searchQueryTimeoutSec(esConfig.getSearchQueryTimeoutSec());
      }
      if (esConfig.getSearchQueryAttempts() != null) {
        builder.searchQueryAttempts(esConfig.getSearchQueryAttempts());
      }
      return builder.build();
    }
  }
}
