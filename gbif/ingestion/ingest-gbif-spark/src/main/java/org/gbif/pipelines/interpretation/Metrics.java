package org.gbif.pipelines.interpretation;

import io.prometheus.metrics.core.metrics.Gauge;

public class Metrics {
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
  public static final Gauge COMPLETED_INTERPRETATION_DATASETS =
      Gauge.builder()
          .name("completed_interpretation_datasets")
          .help("Number of completed datasets being interpreted concurrently in a pod")
          .register();

  public static final Gauge COMPLETED_IDENTIFIERS_DATASETS =
      Gauge.builder()
          .name("completed_identifiers_datasets")
          .help("Number of completed datasets being validated for identifiers concurrently")
          .register();

  public static final Gauge COMPLETED_TABLEBUILD_DATASETS =
      Gauge.builder()
          .name("completed_tablebuild_datasets")
          .help("Number of completed datasets being loaded into Hive concurrently")
          .register();

  public static final Gauge COMPLETED_INDEXING_DATASETS =
      Gauge.builder()
          .name("completed_indexing_datasets")
          .help("Number of completed datasets being indexed in ES concurrently")
          .register();

  public static final Gauge COMPLETED_FRAGMENTER_DATASETS =
      Gauge.builder()
          .name("completed_fragmenter_datasets")
          .help("Number of completed datasets being loaded into hbase tables concurrently")
          .register();
}
