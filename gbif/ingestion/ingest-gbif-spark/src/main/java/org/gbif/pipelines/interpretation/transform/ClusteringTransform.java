package org.gbif.pipelines.interpretation.transform;

import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.ClusteredInterpreter;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.interpretation.transform.utils.ClusteringServiceFactory;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;

public class ClusteringTransform implements Serializable {

    private final PipelinesConfig config;
    private transient ClusteringService clusteringService;

    private ClusteringTransform(PipelinesConfig config) {
        this.config = config;
    }

    public static ClusteringTransform create(PipelinesConfig config) {
        return new ClusteringTransform(config);
    }

    public Optional<ClusteringRecord> convert(IdentifierRecord source) {

        if (clusteringService == null) {
            this.clusteringService = ClusteringServiceFactory.createSupplier(config).get();
        }

        return Interpretation.from(source)
                .to(
                        ir ->
                                ClusteringRecord.newBuilder()
                                        .setId(ir.getId())
                                        .setCreated(Instant.now().toEpochMilli())
                                        .build())
                .when(ir -> ir.getInternalId() != null)
                .via(ClusteredInterpreter.interpretIsClustered(clusteringService))
                .getOfNullable();
    }
}
