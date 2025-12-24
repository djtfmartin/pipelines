package org.gbif.pipelines.interpretation;

import io.prometheus.client.Gauge;

public class Metrics {
    // Create a gauge metric
    public static final Gauge interpretationCount = Gauge.build()
            .name("concurrent_interpretation_datasets")
            .help("Number of datasets being processed concurrently")
            .register();

//    public static final Gauge identifiersCount = Gauge.build()
//            .name("concurrent_identifiers_datasets")
//            .help("Number of datasets being processed concurrently")
//            .register();


}