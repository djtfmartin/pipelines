package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
@Data
public class LocationInheritedRecord {

  /** Pipelines identifier */
  private String id;

  /** Id of the parent whose fields are inherited */
  private String inheritedFrom;

  private String countryCode;
  private String stateProvince;
  private Double decimalLatitude;
  private Double decimalLongitude;
}
