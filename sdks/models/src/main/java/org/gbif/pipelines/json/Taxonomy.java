package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Taxonomy {
  private String taxonKey;
  private String name;
}
