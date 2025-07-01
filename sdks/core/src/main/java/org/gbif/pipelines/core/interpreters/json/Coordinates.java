package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Represents a geographic coordinate with latitude and longitude. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Coordinates {
  private double lon;
  private double lat;
}
