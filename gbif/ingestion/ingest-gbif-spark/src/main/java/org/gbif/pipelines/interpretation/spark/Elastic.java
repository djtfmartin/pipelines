package org.gbif.pipelines.interpretation.spark;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class Elastic {

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--appName", description = "Application name", required = true)
    private String appName;

    @Parameter(names = "--datasetId", description = "Dataset ID", required = true)
    private String datasetId;

    @Parameter(names = "--attempt", description = "Attempt number", required = true)
    private int attempt;

    @Parameter(
        names = "--esMaxBatchSizeBytes",
        description = "Target Elasticsearch Max Batch Size bytes")
    private Long esMaxBatchSizeBytes = 10_485_760L; // 10mb

    @Parameter(names = "--esMaxBatchSize", description = "Elasticsearch max batch size")
    private long esMaxBatchSize = 1_700L;

    @Parameter(
        names = "--esHosts",
        description = "List of Elasticsearch hosts. Required for the DWCA_TO_ES_INDEX step.")
    private List<String> esHosts;

    @Parameter(
        names = "--esIndexName",
        description = "Name of the Elasticsearch index that will be used to index the records")
    private String esIndexName;

    @Parameter(
        names = "--esAlias",
        description =
            "Name of the Elasticsearch aliases. The index created will be added to this aliases.")
    private List<String> esAlias;

    @Parameter(names = "--esSchemaPath", description = "Path to an occurrence indexing schema")
    private String esSchemaPath = "elasticsearch/es-occurrence-schema.json";

    @Parameter(
        names = "--esUsername",
        description = "Name of the Elasticsearch user for basic auth")
    private String esUsername;

    @Parameter(
        names = "--esPassword",
        description = "Name of the Elasticsearch password for basic auth")
    private String esPassword;

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

    @Parameter(names = "--esDocumentId", description = "Elasticsearch document id")
    private String esDocumentId = "GBIF_ID";

    @Parameter(
        names = "--backPressure",
        description = "Limit number of pushing queries at the time")
    private Integer backPressure = -1;

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

  public static void main(String[] argsv) throws IOException {

    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    PipelinesConfig config = loadConfig(args.properties);
    String inputPath =
        String.format("%s/%s/%d/%s", config.getOutputPath(), args.datasetId, args.attempt, "json");

    // Create Spark session
    SparkSession.Builder sparkBuilder =
        SparkSession.builder()
            .appName(args.appName)
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1")
            .config("es.nodes", String.join(",", args.esHosts));


  if (args.master != null && !args.master.isEmpty()) {
      sparkBuilder = sparkBuilder.master(args.master);
  }
  SparkSession spark = sparkBuilder.getOrCreate();

    // --- Create ES index with mapping if not exists ---
    createIndexWithMapping(args.esHosts, args.esIndexName, "es-occurrence-schema.json");

    // Read parquet files
    Dataset<Row> df = spark.read().parquet(inputPath);

    // Optional: print schema
    df.printSchema();

    // Write to Elasticsearch
    df.write()
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", args.esIndexName)
        .option("es.mapping.id", "id")
        .option("es.nodes.wan.only", "true")
        .mode("append")
        .save();

    spark.stop();
  }

    private static void createIndexWithMapping(
            List<String> hosts, String indexName, String mappingFile) throws IOException {

        // Read JSON mapping from file
        InputStream inputStream = Elastic.class.getResourceAsStream("/es-occurrence-schema.json");
        if (inputStream == null) {
            throw new RuntimeException("File not found in classpath: /es-occurrence-schema.json");
        }
        String mappingJson = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

        // Build REST client
        RestClientBuilder builder =
                RestClient.builder(
                        hosts.stream()
                                .map(h -> {
                                    String[] parts = h.split(":");
                                    return new org.apache.http.HttpHost(parts[0], Integer.parseInt(parts[1]), "http");
                                })
                                .toArray(org.apache.http.HttpHost[]::new));
        try (RestClient restClient = builder.build()) {

            // Check if index exists
            Response existsResponse = restClient.performRequest(new Request("HEAD", "/" + indexName));
            if (existsResponse.getStatusLine().getStatusCode() == 404) {
                // Create index with mapping
                Request createReq = new Request("PUT", "/" + indexName);
                createReq.setJsonEntity(mappingJson);
                restClient.performRequest(createReq);
                System.out.println("Created index " + indexName + " with mapping.");
            } else {
                System.out.println("Index " + indexName + " already exists. Skipping creation.");
            }
        }
    }
}
