package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class DerivedMetadataRecord {
    String wktConvexHull;
    EventDate temporalCoverage;
    List<GbifClassification> getTaxonomicCoverage;
}
