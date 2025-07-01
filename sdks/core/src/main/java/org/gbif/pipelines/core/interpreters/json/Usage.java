package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Represents a taxonomic usage, including name parts and authorship. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Usage {
  private String key;
  private String name;
  private String rank;
  private String code;
  private String genericName;
  private String authorship;
  private String infragenericEpithet;
  private String specificEpithet;
  private String infraspecificEpithet;
  private String formattedName;
}
