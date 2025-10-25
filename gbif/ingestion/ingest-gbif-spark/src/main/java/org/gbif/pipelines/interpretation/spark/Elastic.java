package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.EsIndexUtils;

/**
 * Main class for indexing occurrence data to Elasticsearch. It reads Parquet files from HDFS,
 * creates an Elasticsearch index if it doesn't exist, deletes existing records for a specific
 * dataset ID, and writes new records to the index.
 */
public class Elastic {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(names = "--esHosts", description = "List of Elasticsearch hosts")
    private List<String> esHosts;

    @Parameter(
        names = "--esIndexName",
        description = "Name of the Elasticsearch index that will be used to index the records")
    private String esIndexName;

    @Parameter(
        names = "--esAlias",
        description =
            "Name of the Elasticsearch aliases. The index created will be added to these aliases")
    private List<String> esAlias;

    @Parameter(names = "--esSchemaPath", description = "Path to an occurrence indexing schema")
    private String esSchemaPath = "elasticsearch/es-occurrence-schema.json";

    @Parameter(
        names = "--indexRefreshInterval",
        description = "How often to perform a refresh operation, defaults to 30s")
    private String indexRefreshInterval = "40s";

    @Parameter(
        names = "--indexNumberShards",
        description = "Number of primary shards in the target index. Default = 3")
    private Integer indexNumberShards = 3;

    @Parameter(
        names = "--indexNumberReplicas",
        description = "Number of replica shards per primary shard in the target index. Default = 1")
    private Integer indexNumberReplicas = 1;

    @Parameter(
        names = "--esMaxBatchSizeBytes",
        description = "Number of replica shards per primary shard in the target index. Default = 1")
    private Long esMaxBatchSizeBytes = 10_485_760L;

    @Parameter(
        names = "--esMaxBatchSize",
        description = "Number of replica shards per primary shard in the target index. Default = 1")
    private Long esMaxBatchSize = 1_700L;

    @Parameter(
        names = "--searchQueryTimeoutSec",
        description = "Elasticsearch empty delete index query timeout in seconds")
    private Integer searchQueryTimeoutSec = 5;

    @Parameter(
        names = "--searchQueryAttempts",
        description = "Elasticsearch empty index query attempts")
    private Integer searchQueryAttempts = 200;

    @Parameter(
        names = "--indexMaxResultWindow",
        description = "Elasticsearch index max result window")
    private Integer indexMaxResultWindow = 200_000;

    @Parameter(
        names = "--unassignedNodeDelay",
        description = "Elasticsearch unassigned node delay timeout")
    private String unassignedNodeDelay = "5m";

    @Parameter(names = "--useSlowlog", description = "Use search slowlogs")
    private boolean useSlowlog = true;

    @Parameter(
        names = "--indexSearchSlowlogThresholdQueryWarn",
        description = "Index search slowlog threshold query warn")
    private String indexSearchSlowlogThresholdQueryWarn = "20s";

    @Parameter(
        names = "--indexSearchSlowlogThresholdQueryInfo",
        description = "Index search slowlog threshold query info")
    private String indexSearchSlowlogThresholdQueryInfo = "10s";

    @Parameter(
        names = "--indexSearchSlowlogThresholdFetchWarn",
        description = "Index search slowlog threshold fetch warn")
    private String indexSearchSlowlogThresholdFetchWarn = "2s";

    @Parameter(
        names = "--indexSearchSlowlogThresholdFetchInfo",
        description = "Index search slowlog threshold fetch info")
    private String indexSearchSlowlogThresholdFetchInfo = "1s";

    @Parameter(names = "--indexSearchSlowlogLevel", description = "Index search slowlog level")
    private String indexSearchSlowlogLevel = "info";

    @Parameter(names = "--coreSiteConfig", description = "Path to core-site.xml", required = false)
    private String coreSiteConfig;

    @Parameter(names = "--hdfsSiteConfig", description = "Path to hdfs-site.xml", required = false)
    private String hdfsSiteConfig;

    @Parameter(names = "--properties", description = "Path to YAML file", required = true)
    private String properties;

    //    @Parameter(names = "--numberOfShards", description = "Number of shards", required = false)
    //    private int numberOfShards = 10;

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

    runIndexing(config, args.datasetId, args.attempt, args.appName, args.master, args);
  }

    private static void runIndexing(PipelinesConfig config, String datasetId, Integer attempt, String appName, String master, Args args) {
        String inputPath =
            String.format("%s/%s/%d/%s", config.getOutputPath(), datasetId, attempt, "json");

        // Create Spark session
        SparkSession.Builder sparkBuilder =
            SparkSession.builder()
                .appName(appName)
                .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1")
                .config("es.nodes", String.join(",", config.getEsConfig().getEsHosts()));

        if (master != null && !master.isEmpty()) {
          sparkBuilder = sparkBuilder.master(master);
        }

        SparkSession spark = sparkBuilder.getOrCreate();

        ElasticOptions options = ElasticOptions.fromArgsAndConfig(args);

        // Create ES index and alias if not exists
        EsIndexUtils.createIndexAndAliasForDefault(options);

        // Returns indices names in case of swapping
        Set<String> indices = EsIndexUtils.deleteRecordsByDatasetId(options);

        // Read parquet files
        Dataset<Row> df = spark.read().parquet(inputPath);
        //    df.repartition(args.numberOfShards);

        // Write to Elasticsearch
        df.write()
            .format("org.elasticsearch.spark.sql")
            .option("es.resource", args.esIndexName)
            .option("es.batch.size.entries", args.esMaxBatchSize)
            .option("es.batch.size.bytes", args.esMaxBatchSizeBytes)
            .option("es.mapping.id", "id")
            .option("es.nodes.wan.only", "true")
            .option("es.batch.write.refresh", "false")
            .mode("append")
            .save();

        spark.stop();

        EsIndexUtils.updateAlias(options, indices, config != null ? config.getIndexLock() : null);
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

    public static ElasticOptions fromArgsAndConfig(Elastic.Args args) {
      return ElasticOptions.builder()
          .esSchemaPath(args.esSchemaPath)
          .esHosts(args.esHosts.toArray(new String[0]))
          .esIndexName(args.esIndexName)
          .esAlias(args.esAlias.toArray(new String[0]))
          .datasetId(args.datasetId)
          .attempt(args.attempt)
          .indexNumberShards(args.indexNumberShards)
          .indexRefreshInterval(args.indexRefreshInterval)
          .indexNumberReplicas(args.indexNumberReplicas)
          .indexMaxResultWindow(args.indexMaxResultWindow)
          .unassignedNodeDelay(args.unassignedNodeDelay)
          .useSlowlog(args.useSlowlog)
          .indexSearchSlowlogThresholdQueryWarn(args.indexSearchSlowlogThresholdQueryWarn)
          .indexSearchSlowlogThresholdQueryInfo(args.indexSearchSlowlogThresholdQueryInfo)
          .indexSearchSlowlogThresholdFetchWarn(args.indexSearchSlowlogThresholdFetchWarn)
          .indexSearchSlowlogThresholdFetchInfo(args.indexSearchSlowlogThresholdFetchInfo)
          .indexSearchSlowlogLevel(args.indexSearchSlowlogLevel)
          .searchQueryTimeoutSec(args.searchQueryTimeoutSec)
          .searchQueryAttempts(args.searchQueryAttempts)
          .build();
    }
  }
}
