package org.gbif.pipelines.core.interpreters.json;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class DerivedMetadataRecord {
  String wktConvexHull;
  EventDate temporalCoverage;
  List<GbifClassification> getTaxonomicCoverage;
}
