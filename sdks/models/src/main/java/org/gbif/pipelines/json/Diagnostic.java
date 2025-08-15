package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

/** Represents diagnostic information related to taxonomy or classification. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Diagnostic {
  private String matchType;
  private String note;
  private String status;
}
