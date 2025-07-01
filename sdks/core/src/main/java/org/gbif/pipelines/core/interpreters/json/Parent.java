package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Parent {
  private String id;
  private String eventType;
}
