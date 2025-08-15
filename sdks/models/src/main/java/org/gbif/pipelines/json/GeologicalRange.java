package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

/** Represents a geological time range. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class GeologicalRange {
  private Float gt;
  private Float lte;
}
