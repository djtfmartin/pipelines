package org.gbif.pipelines.core.interpreters.json;

import lombok.Builder;
import lombok.Data;

/** Represents a multimedia resource, such as an image, video, or audio clip. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Multimedia {
  private String type;
  private String format;
  private String identifier;
  private String audience;
  private String contributor;
  private String created;
  private String creator;
  private String description;
  private String license;
  private String publisher;
  private String references;
  private String rightsHolder;
  private String source;
  private String title;
  private String datasetId;
}
