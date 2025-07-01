package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Represents a scientific name with a rank and key. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class RankedName {
  private String key;
  private String name;
  private String rank;
}
