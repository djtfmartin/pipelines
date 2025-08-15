package org.gbif.pipelines.json;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Parent {
  private String id;
  private String eventType;
}
