package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

/** Parsed scientific name structure used in taxonomy. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class ParsedName {
  private Boolean abbreviated;
  private Boolean autonym;
  private Authorship basionymAuthorship;
  private Boolean binomial;
  private Boolean candidatus;
  private String code;
  private Authorship combinationAuthorship;
  private Boolean doubtful;
  private String genericName;
  private String genus;
  private Boolean incomplete;
  private Boolean indetermined;
  private String infraspecificEpithet;
  private String notho;
  private String rank;
  private String specificEpithet;
  private String state;
  private String terminalEpithet;
  private Boolean trinomial;
  private String type;
  private String uninomial;
}
