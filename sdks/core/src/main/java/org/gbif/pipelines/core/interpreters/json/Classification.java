package org.gbif.pipelines.core.interpreters.json;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/** General taxonomy classification. */
@Data
@Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
public class Classification {
  private Usage usage;
  private Usage acceptedUsage;
  private String status;
  private Map<String, String> classification;
  private Map<String, String> classificationKeys;
  private Map<String, String> classificationDepth;
  private List<String> taxonKeys;
  private String iucnRedListCategoryCode;
  private List<String> issues;
}
