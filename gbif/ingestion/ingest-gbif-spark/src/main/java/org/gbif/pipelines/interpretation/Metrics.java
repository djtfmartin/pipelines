package org.gbif.pipelines.interpretation;

import io.prometheus.client.Gauge;

public class Metrics {
  // Create a gauge metric
  public static final Gauge interpretationCount =
      Gauge.build()
          .name("concurrent_interpretation_datasets")
          .help("Number of datasets being processed concurrently")
          .register();

  public static final Gauge identifiersCount =
      Gauge.build()
          .name("concurrent_identifiers_datasets")
          .help("Number of datasets being processed concurrently")
          .register();

  public static final Gauge tableBuildCount =
      Gauge.build()
          .name("concurrent_tablebuild_datasets")
          .help("Number of datasets being processed concurrently")
          .register();

  public static final Gauge indexingCount =
      Gauge.build()
          .name("concurrent_indexing_datasets")
          .help("Number of datasets being processed concurrently")
          .register();

  public static final Gauge fragmenterCount =
      Gauge.build()
          .name("concurrent_fragmenter_datasets")
          .help("Number of datasets being processed concurrently")
          .register();
}
