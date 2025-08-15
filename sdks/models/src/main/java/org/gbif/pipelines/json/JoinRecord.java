package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
@Data
public class JoinRecord {
  private String name;
  private String parent;
}
