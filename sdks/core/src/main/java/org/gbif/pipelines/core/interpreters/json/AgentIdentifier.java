package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class AgentIdentifier {
  private String type;
  private String value;
}
