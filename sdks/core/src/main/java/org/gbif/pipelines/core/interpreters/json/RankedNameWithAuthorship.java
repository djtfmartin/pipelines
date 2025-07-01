package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Ranked name with authorship. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class RankedNameWithAuthorship {
  private String key;
  private String name;
  private String rank;
  private String authorship;
}
