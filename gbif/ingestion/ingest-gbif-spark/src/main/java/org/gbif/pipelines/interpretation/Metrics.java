package org.gbif.pipelines.interpretation;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;

public class Metrics {

  public static final Gauge LAST_CONSUMED_MESSAGE_FROM_QUEUE_MS =
      Gauge.builder()
          .name("last_consumed_dataset_timestamp_milliseconds")
          .help("Timestamp of the last consumed dataset message from the queue in milliseconds")
          .register();

  public static final Gauge LAST_COMPLETED_MESSAGE_MS =
      Gauge.builder()
          .name("last_completed_dataset_timestamp_milliseconds")
          .help("Timestamp of the last completed dataset message in milliseconds")
          .register();

  // Create a gauge metric
  public static final Gauge CONCURRENT_INTERPRETATION_DATASETS =
      Gauge.builder()
          .name("concurrent_interpretation_datasets")
          .help("Number of datasets being interpreted concurrently in a pod")
          .register();

  public static final Gauge CONCURRENT_IDENTIFIERS_DATASETS =
      Gauge.builder()
          .name("concurrent_identifiers_datasets")
          .help("Number of datasets being validated for identifiers concurrently")
          .register();

  public static final Gauge CONCURRENT_TABLEBUILD_DATASETS =
      Gauge.builder()
          .name("concurrent_tablebuild_datasets")
          .help("Number of datasets being loaded into Hive concurrently")
          .register();

  public static final Gauge CONCURRENT_INDEXING_DATASETS =
      Gauge.builder()
          .name("concurrent_indexing_datasets")
          .help("Number of datasets being indexed in ES concurrently")
          .register();

  public static final Gauge CONCURRENT_FRAGMENTER_DATASETS =
      Gauge.builder()
          .name("concurrent_fragmenter_datasets")
          .help("Number of datasets being loaded into hbase tables concurrently")
          .register();

  // Create a gauge metric
  public static final Counter COMPLETED_INTERPRETATION_DATASETS =
      Counter.builder()
          .name("completed_interpretation_datasets")
          .help("Number of completed datasets being interpreted concurrently in a pod")
          .register();

  public static final Counter COMPLETED_IDENTIFIERS_DATASETS =
      Counter.builder()
          .name("completed_identifiers_datasets")
          .help("Number of completed datasets being validated for identifiers concurrently")
          .register();

  public static final Counter COMPLETED_TABLEBUILD_DATASETS =
      Counter.builder()
          .name("completed_tablebuild_datasets")
          .help("Number of completed datasets being loaded into Hive concurrently")
          .register();

  public static final Counter COMPLETED_INDEXING_DATASETS =
      Counter.builder()
          .name("completed_indexing_datasets")
          .help("Number of completed datasets being indexed in ES concurrently")
          .register();

  public static final Counter COMPLETED_FRAGMENTER_DATASETS =
      Counter.builder()
          .name("completed_fragmenter_datasets")
          .help("Number of completed datasets being loaded into hbase tables concurrently")
          .register();
}
